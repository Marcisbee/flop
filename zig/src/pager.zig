const std = @import("std");
const pg = @import("page.zig");
const Page = pg.Page;
const PAGE_SIZE = pg.PAGE_SIZE;

const DEFAULT_CACHE_CAP: usize = 10000;

pub const Pager = struct {
    file: std.fs.File,
    allocator: std.mem.Allocator,
    cache: std.AutoHashMap(u64, *Page),
    dirty: std.AutoHashMap(u64, void),
    root_page_id: u64,
    page_count: u64,
    next_row_id: u64,
    free_list_head: u64,

    const Self = @This();

    pub fn open(allocator: std.mem.Allocator, path: []const u8) !Self {
        const file = try std.fs.cwd().createFile(path, .{
            .read = true,
            .truncate = false,
        });

        var self = Self{
            .file = file,
            .allocator = allocator,
            .cache = std.AutoHashMap(u64, *Page).init(allocator),
            .dirty = std.AutoHashMap(u64, void).init(allocator),
            .root_page_id = 0,
            .page_count = 0,
            .next_row_id = 1,
            .free_list_head = 0,
        };

        const stat = try file.stat();
        if (stat.size == 0) {
            // Initialize meta page (page 0)
            const meta = try self.allocPage(.meta);
            _ = meta;
            try self.writeMeta();
            try self.flushAll();
        } else {
            try self.readMeta();
        }

        return self;
    }

    pub fn close(self: *Self) void {
        self.flushAll() catch {};
        // Free cached pages
        var it = self.cache.valueIterator();
        while (it.next()) |page_ptr| {
            self.allocator.destroy(page_ptr.*);
        }
        self.cache.deinit();
        self.dirty.deinit();
        self.file.close();
    }

    pub fn readPage(self: *Self, id: u64) !*Page {
        if (self.cache.get(id)) |cached| {
            return cached;
        }

        const page_ptr = try self.allocator.create(Page);
        const offset = id * PAGE_SIZE;
        const bytes_read = try self.file.preadAll(&page_ptr.data, offset);
        if (bytes_read < PAGE_SIZE) {
            // Zero-fill if file is too short
            @memset(page_ptr.data[bytes_read..], 0);
        }

        try self.cache.put(id, page_ptr);
        return page_ptr;
    }

    pub fn writePage(self: *Self, id: u64) !void {
        try self.dirty.put(id, {});
    }

    pub fn allocPage(self: *Self, page_type: pg.PageType) !*Page {
        // Try free list first
        if (self.free_list_head != 0) {
            const id = self.free_list_head;
            const page = try self.readPage(id);
            self.free_list_head = page.overflowId();
            page.clearEntries();
            page.data[4] = @intFromEnum(page_type);
            std.mem.writeInt(u64, page.data[8..16], id, .little);
            page.setOverflowId(0);
            try self.writePage(id);
            return page;
        }

        self.page_count += 1;
        const id = self.page_count;
        const page_ptr = try self.allocator.create(Page);
        page_ptr.* = Page.init(page_type, id);
        try self.cache.put(id, page_ptr);
        try self.writePage(id);
        return page_ptr;
    }

    pub fn freePage(self: *Self, id: u64) !void {
        const page = try self.readPage(id);
        page.data[4] = @intFromEnum(pg.PageType.free);
        page.setOverflowId(self.free_list_head);
        self.free_list_head = id;
        try self.writePage(id);
    }

    /// Meta page layout (page 0):
    ///   [24:32]  root page ID
    ///   [32:40]  page count
    ///   [40:48]  next row ID
    ///   [48:56]  free list head
    pub fn readMeta(self: *Self) !void {
        const page = try self.readPage(0);
        self.root_page_id = std.mem.readInt(u64, page.data[24..32], .little);
        self.page_count = std.mem.readInt(u64, page.data[32..40], .little);
        self.next_row_id = std.mem.readInt(u64, page.data[40..48], .little);
        self.free_list_head = std.mem.readInt(u64, page.data[48..56], .little);
    }

    pub fn writeMeta(self: *Self) !void {
        const page = try self.readPage(0);
        std.mem.writeInt(u64, page.data[24..32], self.root_page_id, .little);
        std.mem.writeInt(u64, page.data[32..40], self.page_count, .little);
        std.mem.writeInt(u64, page.data[40..48], self.next_row_id, .little);
        std.mem.writeInt(u64, page.data[48..56], self.free_list_head, .little);
        try self.writePage(0);
    }

    pub fn flushAll(self: *Self) !void {
        var it = self.dirty.keyIterator();
        while (it.next()) |id_ptr| {
            const id = id_ptr.*;
            if (self.cache.get(id)) |page| {
                page.computeChecksum();
                const offset = id * PAGE_SIZE;
                try self.file.pwriteAll(&page.data, offset);
            }
        }
        self.dirty.clearRetainingCapacity();
        try self.file.sync();
    }

    pub fn nextRowId(self: *Self) u64 {
        const id = self.next_row_id;
        self.next_row_id += 1;
        return id;
    }
};

test "pager open and close" {
    const allocator = std.testing.allocator;
    const test_path = "/tmp/flop_pager_test.db";

    // Clean up any previous test file
    std.fs.cwd().deleteFile(test_path) catch {};

    var pager = try Pager.open(allocator, test_path);
    defer {
        pager.close();
        std.fs.cwd().deleteFile(test_path) catch {};
    }

    try std.testing.expect(pager.page_count >= 0);
    try std.testing.expectEqual(@as(u64, 1), pager.next_row_id);
}

test "pager alloc and read page" {
    const allocator = std.testing.allocator;
    const test_path = "/tmp/flop_pager_alloc_test.db";

    std.fs.cwd().deleteFile(test_path) catch {};

    var pager = try Pager.open(allocator, test_path);
    defer {
        pager.close();
        std.fs.cwd().deleteFile(test_path) catch {};
    }

    const page = try pager.allocPage(.leaf);
    const id = page.pageId();
    try std.testing.expect(id > 0);
    try std.testing.expectEqual(pg.PageType.leaf, page.pageType());

    // Should be able to read same page back
    const page2 = try pager.readPage(id);
    try std.testing.expectEqual(id, page2.pageId());
}

test "pager free list" {
    const allocator = std.testing.allocator;
    const test_path = "/tmp/flop_pager_freelist_test.db";

    std.fs.cwd().deleteFile(test_path) catch {};

    var pager = try Pager.open(allocator, test_path);
    defer {
        pager.close();
        std.fs.cwd().deleteFile(test_path) catch {};
    }

    const page = try pager.allocPage(.leaf);
    const id = page.pageId();
    const count_before = pager.page_count;

    try pager.freePage(id);
    try std.testing.expectEqual(id, pager.free_list_head);

    // Allocating again should reuse the freed page
    const page2 = try pager.allocPage(.internal);
    try std.testing.expectEqual(id, page2.pageId());
    try std.testing.expectEqual(count_before, pager.page_count);
}

test "pager flush and reopen" {
    const allocator = std.testing.allocator;
    const test_path = "/tmp/flop_pager_flush_test.db";

    std.fs.cwd().deleteFile(test_path) catch {};

    // Open, write data, close
    {
        var pager = try Pager.open(allocator, test_path);
        const page = try pager.allocPage(.leaf);
        _ = page.appendEntry("hello", "world");
        try pager.writeMeta();
        try pager.flushAll();
        pager.close();
    }

    // Reopen and verify
    {
        var pager = try Pager.open(allocator, test_path);
        defer {
            pager.close();
            std.fs.cwd().deleteFile(test_path) catch {};
        }
        try std.testing.expect(pager.page_count >= 1);
    }
}

test "pager next row id" {
    const allocator = std.testing.allocator;
    const test_path = "/tmp/flop_pager_rowid_test.db";

    std.fs.cwd().deleteFile(test_path) catch {};

    var pager = try Pager.open(allocator, test_path);
    defer {
        pager.close();
        std.fs.cwd().deleteFile(test_path) catch {};
    }

    const id1 = pager.nextRowId();
    const id2 = pager.nextRowId();
    const id3 = pager.nextRowId();
    try std.testing.expectEqual(@as(u64, 1), id1);
    try std.testing.expectEqual(@as(u64, 2), id2);
    try std.testing.expectEqual(@as(u64, 3), id3);
}