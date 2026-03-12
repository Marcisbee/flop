const std = @import("std");

pub const AuthManager = struct {
    allocator: std.mem.Allocator,
    tokens: std.StringHashMap(u64),
    mutex: std.Thread.Mutex,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .allocator = allocator,
            .tokens = std.StringHashMap(u64).init(allocator),
            .mutex = .{},
        };
    }

    pub fn deinit(self: *Self) void {
        // Free all token keys
        var it = self.tokens.keyIterator();
        while (it.next()) |key_ptr| {
            self.allocator.free(key_ptr.*);
        }
        self.tokens.deinit();
    }

    fn bytesToHex(bytes: []const u8, out: []u8) []const u8 {
        const hex_chars = "0123456789abcdef";
        for (bytes, 0..) |b, i| {
            out[i * 2] = hex_chars[b >> 4];
            out[i * 2 + 1] = hex_chars[b & 0xf];
        }
        return out[0 .. bytes.len * 2];
    }

    pub fn makeToken(self: *Self, user_id: u64) ![]const u8 {
        var random_bytes: [24]u8 = undefined;
        std.crypto.random.bytes(&random_bytes);

        var hex_buf: [48]u8 = undefined;
        const hex = bytesToHex(&random_bytes, &hex_buf);

        // Create token string: hex.userid
        var token_buf: [80]u8 = undefined;
        const token = std.fmt.bufPrint(&token_buf, "{s}.{d}", .{ hex, user_id }) catch unreachable;

        const token_owned = try self.allocator.dupe(u8, token);

        self.mutex.lock();
        defer self.mutex.unlock();
        try self.tokens.put(token_owned, user_id);

        return token_owned;
    }

    pub fn validateToken(self: *Self, token: []const u8) ?u64 {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.tokens.get(token);
    }

    pub fn sha256Hex(input: []const u8) [64]u8 {
        var hash: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(input, &hash, .{});
        var hex: [64]u8 = undefined;
        _ = bytesToHex(&hash, &hex);
        return hex;
    }
};