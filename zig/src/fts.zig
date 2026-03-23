const std = @import("std");

/// Prefix-aware full-text search index with BM25 scoring.
pub const FtsIndex = struct {
    allocator: std.mem.Allocator,
    /// token -> list of (doc_id, frequency) pairs
    postings: std.StringHashMap(std.array_list.Managed(Posting)),
    /// doc_id -> token count
    doc_lengths: std.AutoHashMap(u64, u32),
    total_docs: u32,
    total_doc_len: u64,
    sorted_terms: std.array_list.Managed([]const u8),
    terms_dirty: bool,

    const Posting = struct {
        doc_id: u64,
        freq: u16,
    };

    const SearchState = struct {
        score: f64 = 0,
        matched_tokens: u32 = 0,
        last_token_ix: usize = std.math.maxInt(usize),
    };

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .allocator = allocator,
            .postings = std.StringHashMap(std.array_list.Managed(Posting)).init(allocator),
            .doc_lengths = std.AutoHashMap(u64, u32).init(allocator),
            .total_docs = 0,
            .total_doc_len = 0,
            .sorted_terms = std.array_list.Managed([]const u8).init(allocator),
            .terms_dirty = true,
        };
    }

    pub fn deinit(self: *Self) void {
        var it = self.postings.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            entry.value_ptr.deinit();
        }
        self.postings.deinit();
        self.doc_lengths.deinit();
        self.sorted_terms.deinit();
    }

    fn tokenize(text: []const u8, allocator: std.mem.Allocator) !std.array_list.Managed([]const u8) {
        var tokens = std.array_list.Managed([]const u8).init(allocator);
        var start: usize = 0;
        var in_word = false;

        for (text, 0..) |c, i| {
            const is_alnum = std.ascii.isAlphanumeric(c);
            if (is_alnum) {
                if (!in_word) start = i;
                in_word = true;
            } else {
                if (in_word and i - start >= 2) {
                    try tokens.append(text[start..i]);
                }
                in_word = false;
            }
        }
        if (in_word and text.len - start >= 2) {
            try tokens.append(text[start..]);
        }
        return tokens;
    }

    pub fn indexDoc(self: *Self, doc_id: u64, text: []const u8) !void {
        if (doc_id == 0) return;

        self.removeDoc(doc_id);

        var tokens = try tokenize(text, self.allocator);
        defer tokens.deinit();

        var freq_map = std.StringHashMap(u16).init(self.allocator);
        defer {
            var kit = freq_map.keyIterator();
            while (kit.next()) |key_ptr| {
                self.allocator.free(key_ptr.*);
            }
            freq_map.deinit();
        }

        for (tokens.items) |tok| {
            const lower = try lowerDup(self.allocator, tok);
            if (freq_map.getPtr(lower)) |ptr| {
                ptr.* += 1;
                self.allocator.free(lower);
            } else {
                try freq_map.put(lower, 1);
            }
        }

        var it = freq_map.iterator();
        while (it.next()) |entry| {
            const term = entry.key_ptr.*;
            const freq = entry.value_ptr.*;

            const gop = try self.postings.getOrPut(term);
            if (!gop.found_existing) {
                gop.key_ptr.* = try self.allocator.dupe(u8, term);
                gop.value_ptr.* = std.array_list.Managed(Posting).init(self.allocator);
                self.terms_dirty = true;
            }
            try gop.value_ptr.append(.{ .doc_id = doc_id, .freq = freq });
        }

        const doc_len: u32 = @intCast(tokens.items.len);
        try self.doc_lengths.put(doc_id, doc_len);
        self.total_docs += 1;
        self.total_doc_len += doc_len;
    }

    pub fn removeDoc(self: *Self, doc_id: u64) void {
        const old_len = self.doc_lengths.get(doc_id) orelse return;

        self.total_doc_len -= old_len;
        self.total_docs -= 1;
        _ = self.doc_lengths.remove(doc_id);

        var empty_terms = std.array_list.Managed([]const u8).init(self.allocator);
        defer empty_terms.deinit();

        var it = self.postings.iterator();
        while (it.next()) |entry| {
            var list = entry.value_ptr;
            var i: usize = 0;
            while (i < list.items.len) {
                if (list.items[i].doc_id == doc_id) {
                    _ = list.orderedRemove(i);
                } else {
                    i += 1;
                }
            }

            if (list.items.len == 0) {
                empty_terms.append(entry.key_ptr.*) catch {};
            }
        }

        for (empty_terms.items) |term| {
            if (self.postings.fetchRemove(term)) |removed| {
                self.allocator.free(removed.key);
                removed.value.deinit();
                self.terms_dirty = true;
            }
        }
    }

    /// Search with BM25 scoring. Query tokens match indexed terms by prefix.
    pub fn search(self: *Self, query: []const u8, limit: usize) ![]const SearchResult {
        var query_tokens = try tokenize(query, self.allocator);
        defer query_tokens.deinit();

        if (query_tokens.items.len == 0 or limit == 0 or self.total_docs == 0) return &.{};

        try self.ensureSortedTerms();

        var lowered = std.array_list.Managed([]const u8).init(self.allocator);
        defer {
            for (lowered.items) |tok| self.allocator.free(tok);
            lowered.deinit();
        }

        for (query_tokens.items) |tok| {
            try lowered.append(try lowerDup(self.allocator, tok));
        }

        const avg_dl: f64 =
            @as(f64, @floatFromInt(self.total_doc_len)) / @as(f64, @floatFromInt(self.total_docs));
        const k1: f64 = 1.2;
        const b: f64 = 0.75;
        const n_f: f64 = @floatFromInt(self.total_docs);

        var states = std.AutoHashMap(u64, SearchState).init(self.allocator);
        defer states.deinit();

        for (lowered.items, 0..) |prefix, token_ix| {
            var matched_any = false;
            const start = lowerBound(self.sorted_terms.items, prefix);
            var term_ix = start;
            while (term_ix < self.sorted_terms.items.len) : (term_ix += 1) {
                const term = self.sorted_terms.items[term_ix];
                if (!std.mem.startsWith(u8, term, prefix)) break;

                matched_any = true;
                const posting_list = self.postings.get(term) orelse continue;
                const df_f: f64 = @floatFromInt(posting_list.items.len);
                const idf = @log((n_f - df_f + 0.5) / (df_f + 0.5) + 1.0);

                for (posting_list.items) |posting| {
                    const tf_f: f64 = @floatFromInt(posting.freq);
                    const dl_f: f64 = @floatFromInt(self.doc_lengths.get(posting.doc_id) orelse 1);
                    const tf_norm = (tf_f * (k1 + 1.0)) / (tf_f + k1 * (1.0 - b + b * dl_f / avg_dl));
                    const gop = try states.getOrPut(posting.doc_id);
                    if (!gop.found_existing) {
                        gop.value_ptr.* = .{};
                    }
                    gop.value_ptr.score += idf * tf_norm;
                    if (gop.value_ptr.last_token_ix != token_ix) {
                        gop.value_ptr.last_token_ix = token_ix;
                        gop.value_ptr.matched_tokens += 1;
                    }
                }
            }

            if (!matched_any) return &.{};
        }

        var results = std.array_list.Managed(SearchResult).init(self.allocator);
        defer results.deinit();

        var it = states.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.matched_tokens == lowered.items.len) {
                try results.append(.{
                    .doc_id = entry.key_ptr.*,
                    .score = entry.value_ptr.score,
                });
            }
        }

        std.mem.sortUnstable(SearchResult, results.items, {}, struct {
            fn cmp(_: void, a: SearchResult, b_item: SearchResult) bool {
                if (a.score == b_item.score) {
                    return a.doc_id < b_item.doc_id;
                }
                return a.score > b_item.score;
            }
        }.cmp);

        const result_count = @min(limit, results.items.len);
        return try self.allocator.dupe(SearchResult, results.items[0..result_count]);
    }

    fn ensureSortedTerms(self: *Self) !void {
        if (!self.terms_dirty) return;

        self.sorted_terms.clearRetainingCapacity();
        var it = self.postings.keyIterator();
        while (it.next()) |key| {
            try self.sorted_terms.append(key.*);
        }
        std.mem.sortUnstable([]const u8, self.sorted_terms.items, {}, struct {
            fn cmp(_: void, a: []const u8, b: []const u8) bool {
                return std.mem.order(u8, a, b) == .lt;
            }
        }.cmp);
        self.terms_dirty = false;
    }

    fn lowerDup(allocator: std.mem.Allocator, input: []const u8) ![]u8 {
        var out = try allocator.alloc(u8, input.len);
        for (input, 0..) |c, i| {
            out[i] = std.ascii.toLower(c);
        }
        return out;
    }

    fn lowerBound(items: []const []const u8, prefix: []const u8) usize {
        var left: usize = 0;
        var right: usize = items.len;
        while (left < right) {
            const mid = left + (right - left) / 2;
            const order = std.mem.order(u8, items[mid], prefix);
            if (order == .lt) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        return left;
    }

    pub const SearchResult = struct {
        doc_id: u64,
        score: f64,
    };
};
