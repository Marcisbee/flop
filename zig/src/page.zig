const std = @import("std");

pub const PAGE_SIZE: usize = 4096;
pub const PAGE_HEADER_SIZE: usize = 24;
pub const MAX_KEY_SIZE: usize = 256;
pub const MAX_VAL_SIZE: usize = PAGE_SIZE - PAGE_HEADER_SIZE - 32;

pub const PageType = enum(u8) {
    free = 0,
    internal = 1,
    leaf = 2,
    overflow = 3,
    meta = 4,
};

/// Page layout:
///   [0:4]   CRC32 checksum
///   [4:5]   page type
///   [5:7]   num entries (u16 LE)
///   [7:8]   flags
///   [8:16]  page id (u64 LE)
///   [16:24] overflow page id (u64 LE, 0 = none)
///   [24..]  entries
///
/// Entry layout:
///   [0:2]  key length (u16 LE)
///   [2:4]  value length (u16 LE)
///   [4:4+klen] key bytes
///   [4+klen:4+klen+vlen] value bytes
pub const Page = struct {
    data: [PAGE_SIZE]u8 align(4096),

    const Self = @This();

    pub fn init(page_type: PageType, page_id: u64) Self {
        var p = Self{ .data = [_]u8{0} ** PAGE_SIZE };
        p.data[4] = @intFromEnum(page_type);
        std.mem.writeInt(u64, p.data[8..16], page_id, .little);
        return p;
    }

    pub fn pageType(self: *const Self) PageType {
        return @enumFromInt(self.data[4]);
    }

    pub fn numEntries(self: *const Self) u16 {
        return std.mem.readInt(u16, self.data[5..7], .little);
    }

    pub fn setNumEntries(self: *Self, n: u16) void {
        std.mem.writeInt(u16, self.data[5..7], n, .little);
    }

    pub fn pageId(self: *const Self) u64 {
        return std.mem.readInt(u64, self.data[8..16], .little);
    }

    pub fn overflowId(self: *const Self) u64 {
        return std.mem.readInt(u64, self.data[16..24], .little);
    }

    pub fn setOverflowId(self: *Self, id: u64) void {
        std.mem.writeInt(u64, self.data[16..24], id, .little);
    }

    pub fn flags(self: *const Self) u8 {
        return self.data[7];
    }

    pub fn setFlags(self: *Self, f: u8) void {
        self.data[7] = f;
    }

    pub const Entry = struct {
        key: []const u8,
        value: []const u8,
    };

    /// Get the entry at index `idx`.
    pub fn entryAt(self: *const Self, idx: u16) ?Entry {
        const n = self.numEntries();
        if (idx >= n) return null;

        var offset: usize = PAGE_HEADER_SIZE;
        var i: u16 = 0;
        while (i < idx) : (i += 1) {
            if (offset + 4 > PAGE_SIZE) return null;
            const klen = std.mem.readInt(u16, self.data[offset..][0..2], .little);
            const vlen = std.mem.readInt(u16, self.data[offset + 2 ..][0..2], .little);
            offset += 4 + klen + vlen;
        }

        if (offset + 4 > PAGE_SIZE) return null;
        const klen = std.mem.readInt(u16, self.data[offset..][0..2], .little);
        const vlen = std.mem.readInt(u16, self.data[offset + 2 ..][0..2], .little);
        const key_start = offset + 4;
        const val_start = key_start + klen;
        if (val_start + vlen > PAGE_SIZE) return null;

        return Entry{
            .key = self.data[key_start .. key_start + klen],
            .value = self.data[val_start .. val_start + vlen],
        };
    }

    /// Calculate total used bytes in entries area.
    pub fn usedBytes(self: *const Self) usize {
        const n = self.numEntries();
        var offset: usize = PAGE_HEADER_SIZE;
        var i: u16 = 0;
        while (i < n) : (i += 1) {
            if (offset + 4 > PAGE_SIZE) break;
            const klen = std.mem.readInt(u16, self.data[offset..][0..2], .little);
            const vlen = std.mem.readInt(u16, self.data[offset + 2 ..][0..2], .little);
            offset += 4 + klen + vlen;
        }
        return offset - PAGE_HEADER_SIZE;
    }

    /// Free space remaining for entries.
    pub fn freeSpace(self: *const Self) usize {
        return (PAGE_SIZE - PAGE_HEADER_SIZE) - self.usedBytes();
    }

    /// Append an entry to the end. Returns false if not enough space.
    pub fn appendEntry(self: *Self, key: []const u8, value: []const u8) bool {
        const needed = 4 + key.len + value.len;
        if (needed > self.freeSpace()) return false;

        const n = self.numEntries();
        const offset: usize = PAGE_HEADER_SIZE + self.usedBytes();

        std.mem.writeInt(u16, self.data[offset..][0..2], @intCast(key.len), .little);
        std.mem.writeInt(u16, self.data[offset + 2 ..][0..2], @intCast(value.len), .little);
        @memcpy(self.data[offset + 4 ..][0..key.len], key);
        @memcpy(self.data[offset + 4 + key.len ..][0..value.len], value);

        self.setNumEntries(n + 1);
        return true;
    }

    /// Clear all entries.
    pub fn clearEntries(self: *Self) void {
        self.setNumEntries(0);
        @memset(self.data[PAGE_HEADER_SIZE..], 0);
    }

    /// Compute CRC32 over data[4..] and store in data[0..4].
    pub fn computeChecksum(self: *Self) void {
        const crc = std.hash.crc.Crc32IsoHdlc.hash(self.data[4..]);
        std.mem.writeInt(u32, self.data[0..4], crc, .little);
    }

    /// Verify CRC32 checksum.
    pub fn verifyChecksum(self: *const Self) bool {
        const stored = std.mem.readInt(u32, self.data[0..4], .little);
        const computed = std.hash.crc.Crc32IsoHdlc.hash(self.data[4..]);
        return stored == computed;
    }
};

// ---------- Tests ----------

test "page init and type" {
    var p = Page.init(.leaf, 42);
    try std.testing.expectEqual(PageType.leaf, p.pageType());
    try std.testing.expectEqual(@as(u64, 42), p.pageId());
    try std.testing.expectEqual(@as(u16, 0), p.numEntries());
}

test "append and read entries" {
    var p = Page.init(.leaf, 1);
    try std.testing.expect(p.appendEntry("hello", "world"));
    try std.testing.expect(p.appendEntry("foo", "bar"));
    try std.testing.expectEqual(@as(u16, 2), p.numEntries());

    const e0 = p.entryAt(0).?;
    try std.testing.expectEqualStrings("hello", e0.key);
    try std.testing.expectEqualStrings("world", e0.value);

    const e1 = p.entryAt(1).?;
    try std.testing.expectEqualStrings("foo", e1.key);
    try std.testing.expectEqualStrings("bar", e1.value);

    try std.testing.expectEqual(@as(?Page.Entry, null), p.entryAt(2));
}

test "checksum round trip" {
    var p = Page.init(.leaf, 7);
    _ = p.appendEntry("key", "value");
    p.computeChecksum();
    try std.testing.expect(p.verifyChecksum());

    // Corrupt a byte
    p.data[100] ^= 0xFF;
    try std.testing.expect(!p.verifyChecksum());
}

test "free space accounting" {
    var p = Page.init(.leaf, 1);
    const initial_free = p.freeSpace();
    try std.testing.expectEqual(PAGE_SIZE - PAGE_HEADER_SIZE, initial_free);

    _ = p.appendEntry("k", "v");
    // 4 header + 1 key + 1 value = 6 bytes used
    try std.testing.expectEqual(initial_free - 6, p.freeSpace());
}

test "clear entries" {
    var p = Page.init(.leaf, 1);
    _ = p.appendEntry("a", "b");
    _ = p.appendEntry("c", "d");
    try std.testing.expectEqual(@as(u16, 2), p.numEntries());

    p.clearEntries();
    try std.testing.expectEqual(@as(u16, 0), p.numEntries());
    try std.testing.expectEqual(PAGE_SIZE - PAGE_HEADER_SIZE, p.freeSpace());
}

test "overflow id" {
    var p = Page.init(.leaf, 1);
    try std.testing.expectEqual(@as(u64, 0), p.overflowId());
    p.setOverflowId(99);
    try std.testing.expectEqual(@as(u64, 99), p.overflowId());
}

test "flags" {
    var p = Page.init(.leaf, 1);
    try std.testing.expectEqual(@as(u8, 0), p.flags());
    p.setFlags(0xAB);
    try std.testing.expectEqual(@as(u8, 0xAB), p.flags());
}