const std = @import("std");
const encoding = @import("encoding.zig");

/// A secondary index entry: composite key -> row ID.
pub const IndexEntry = struct {
    key: []const u8,
    row_id: u64,
};

/// In-memory sorted index backed by a dynamic array.
/// Supports unique and non-unique modes.
pub const SecondaryIndex = struct {
    allocator: std.mem.Allocator,
    entries: std.array_list.Managed(IndexEntry),
    is_unique: bool,
    sorted: bool,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, is_unique: bool) Self {
        return Self{
            .allocator = allocator,
            .entries = std.array_list.Managed(IndexEntry).init(allocator),
            .is_unique = is_unique,
            .sorted = true,
        };
    }

    pub fn deinit(self: *Self) void {
        for (self.entries.items) |entry| {
            self.allocator.free(entry.key);
        }
        self.entries.deinit();
    }

    pub fn clear(self: *Self) void {
        for (self.entries.items) |entry| {
            self.allocator.free(entry.key);
        }
        self.entries.clearRetainingCapacity();
        self.sorted = true;
    }

    fn compareKeys(_: void, a: IndexEntry, b: IndexEntry) bool {
        const order = std.mem.order(u8, a.key, b.key);
        if (order != .eq) return order == .lt;
        return a.row_id < b.row_id;
    }

    pub fn ensureSorted(self: *Self) void {
        if (!self.sorted) {
            std.mem.sortUnstable(IndexEntry, self.entries.items, {}, compareKeys);
            self.sorted = true;
        }
    }

    pub fn put(self: *Self, key: []const u8, row_id: u64) !void {
        const key_copy = try self.allocator.dupe(u8, key);
        try self.entries.append(IndexEntry{ .key = key_copy, .row_id = row_id });
        self.sorted = false;
    }

    pub fn remove(self: *Self, key: []const u8, row_id: u64) void {
        self.ensureSorted();
        var i: usize = 0;
        while (i < self.entries.items.len) {
            const e = self.entries.items[i];
            if (std.mem.eql(u8, e.key, key) and e.row_id == row_id) {
                self.allocator.free(e.key);
                _ = self.entries.orderedRemove(i);
                return;
            }
            i += 1;
        }
    }

    /// Find all row IDs matching a key.
    pub fn lookup(self: *Self, key: []const u8) []const IndexEntry {
        self.ensureSorted();
        const start = self.lowerBound(key);
        if (start >= self.entries.items.len or !std.mem.eql(u8, self.entries.items[start].key, key)) {
            return &.{};
        }

        var end = start;
        while (end < self.entries.items.len and std.mem.eql(u8, self.entries.items[end].key, key)) {
            end += 1;
        }
        return self.entries.items[start..end];
    }

    pub fn count(self: *Self) usize {
        return self.entries.items.len;
    }

    pub fn scan(self: *Self, reverse: bool, callback: *const fn (IndexEntry) bool) void {
        self.ensureSorted();

        if (!reverse) {
            for (self.entries.items) |entry| {
                if (!callback(entry)) return;
            }
            return;
        }

        var i = self.entries.items.len;
        while (i > 0) {
            i -= 1;
            if (!callback(self.entries.items[i])) return;
        }
    }

    fn lowerBound(self: *Self, key: []const u8) usize {
        var left: usize = 0;
        var right: usize = self.entries.items.len;
        while (left < right) {
            const mid = left + (right - left) / 2;
            const order = std.mem.order(u8, self.entries.items[mid].key, key);
            if (order == .lt) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        return left;
    }
};
