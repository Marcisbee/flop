const std = @import("std");

/// Encode a u64 as big-endian 8 bytes (preserves sort order).
pub fn encodeU64(buf: *[8]u8, val: u64) void {
    std.mem.writeInt(u64, buf, val, .big);
}

pub fn decodeU64(buf: *const [8]u8) u64 {
    return std.mem.readInt(u64, buf, .big);
}

/// Encode an i64 with sign-bit flip for sortable encoding.
pub fn encodeI64(buf: *[8]u8, val: i64) void {
    const unsigned: u64 = @bitCast(val);
    const flipped = unsigned ^ (@as(u64, 1) << 63);
    std.mem.writeInt(u64, buf, flipped, .big);
}

pub fn decodeI64(buf: *const [8]u8) i64 {
    const raw = std.mem.readInt(u64, buf, .big);
    const unflipped = raw ^ (@as(u64, 1) << 63);
    return @bitCast(unflipped);
}

/// Encode an f64 with IEEE 754 sign-bit transformation for sortable encoding.
pub fn encodeF64(buf: *[8]u8, val: f64) void {
    const bits: u64 = @bitCast(val);
    const encoded = if (bits & (@as(u64, 1) << 63) != 0)
        ~bits // negative: flip all bits
    else
        bits | (@as(u64, 1) << 63); // positive: flip sign bit
    std.mem.writeInt(u64, buf, encoded, .big);
}

pub fn decodeF64(buf: *const [8]u8) f64 {
    const raw = std.mem.readInt(u64, buf, .big);
    const decoded = if (raw & (@as(u64, 1) << 63) != 0)
        raw & ~(@as(u64, 1) << 63) // positive: clear sign bit
    else
        ~raw; // negative: flip all bits back
    return @bitCast(decoded);
}

/// Encode a string with length prefix (2-byte LE length + bytes).
pub fn encodeString(allocator: std.mem.Allocator, s: []const u8) ![]u8 {
    const buf = try allocator.alloc(u8, 2 + s.len);
    std.mem.writeInt(u16, buf[0..2], @intCast(s.len), .little);
    @memcpy(buf[2..][0..s.len], s);
    return buf;
}

pub fn decodeString(buf: []const u8) ?[]const u8 {
    if (buf.len < 2) return null;
    const len = std.mem.readInt(u16, buf[0..2], .little);
    if (buf.len < 2 + len) return null;
    return buf[2 .. 2 + len];
}

/// Row binary format:
///   [0:8]   ID (u64 BE)
///   [8:16]  created_at (i64 sortable)
///   [16:24] updated_at (i64 sortable)
///   [24:32] version (u64 BE)
///   [32..]  field data: [2-byte name_len][name][4-byte data_len][data]...
pub const ROW_HEADER_SIZE: usize = 32;

pub fn encodeRowHeader(buf: *[ROW_HEADER_SIZE]u8, id: u64, created_at: i64, updated_at: i64, version: u64) void {
    encodeU64(buf[0..8], id);
    encodeI64(buf[8..16], created_at);
    encodeI64(buf[16..24], updated_at);
    encodeU64(buf[24..32], version);
}

pub fn decodeRowId(buf: []const u8) ?u64 {
    if (buf.len < 8) return null;
    return decodeU64(buf[0..8]);
}