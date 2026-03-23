const std = @import("std");

/// Simple JSON writer that builds JSON strings.
pub const JsonWriter = struct {
    buf: std.array_list.Managed(u8),

    pub fn init(allocator: std.mem.Allocator) JsonWriter {
        return JsonWriter{ .buf = std.array_list.Managed(u8).init(allocator) };
    }

    pub fn deinit(self: *JsonWriter) void {
        self.buf.deinit();
    }

    pub fn toOwnedSlice(self: *JsonWriter) ![]u8 {
        return self.buf.toOwnedSlice();
    }

    pub fn objectStart(self: *JsonWriter) !void {
        try self.buf.append('{');
    }

    pub fn objectEnd(self: *JsonWriter) !void {
        // Remove trailing comma if present
        if (self.buf.items.len > 0 and self.buf.items[self.buf.items.len - 1] == ',') {
            self.buf.items.len -= 1;
        }
        try self.buf.append('}');
    }

    pub fn arrayStart(self: *JsonWriter) !void {
        try self.buf.append('[');
    }

    pub fn arrayEnd(self: *JsonWriter) !void {
        if (self.buf.items.len > 0 and self.buf.items[self.buf.items.len - 1] == ',') {
            self.buf.items.len -= 1;
        }
        try self.buf.append(']');
    }

    pub fn key(self: *JsonWriter, k: []const u8) !void {
        try self.buf.append('"');
        try self.buf.appendSlice(k);
        try self.buf.appendSlice("\":");
    }

    pub fn stringValue(self: *JsonWriter, v: []const u8) !void {
        try self.buf.append('"');
        // Escape special characters
        for (v) |c| {
            switch (c) {
                '"' => try self.buf.appendSlice("\\\""),
                '\\' => try self.buf.appendSlice("\\\\"),
                '\n' => try self.buf.appendSlice("\\n"),
                '\r' => try self.buf.appendSlice("\\r"),
                '\t' => try self.buf.appendSlice("\\t"),
                else => {
                    if (c < 0x20) {
                        try self.buf.appendSlice("\\u00");
                        const hex_chars = "0123456789abcdef";
                        try self.buf.append(hex_chars[c >> 4]);
                        try self.buf.append(hex_chars[c & 0xf]);
                    } else {
                        try self.buf.append(c);
                    }
                },
            }
        }
        try self.buf.append('"');
        try self.buf.append(',');
    }

    pub fn intValue(self: *JsonWriter, v: i64) !void {
        var num_buf: [32]u8 = undefined;
        const s = std.fmt.bufPrint(&num_buf, "{d}", .{v}) catch unreachable;
        try self.buf.appendSlice(s);
        try self.buf.append(',');
    }

    pub fn uintValue(self: *JsonWriter, v: u64) !void {
        var num_buf: [32]u8 = undefined;
        const s = std.fmt.bufPrint(&num_buf, "{d}", .{v}) catch unreachable;
        try self.buf.appendSlice(s);
        try self.buf.append(',');
    }

    pub fn floatValue(self: *JsonWriter, v: f64) !void {
        var num_buf: [64]u8 = undefined;
        const s = std.fmt.bufPrint(&num_buf, "{d}", .{v}) catch unreachable;
        try self.buf.appendSlice(s);
        try self.buf.append(',');
    }

    pub fn boolValue(self: *JsonWriter, v: bool) !void {
        try self.buf.appendSlice(if (v) "true" else "false");
        try self.buf.append(',');
    }

    pub fn nullValue(self: *JsonWriter) !void {
        try self.buf.appendSlice("null,");
    }

    pub fn comma(self: *JsonWriter) !void {
        try self.buf.append(',');
    }

    pub fn raw(self: *JsonWriter, s: []const u8) !void {
        try self.buf.appendSlice(s);
    }
};

/// Simple JSON parser that extracts string values from a flat JSON object.
pub fn parseJsonObject(allocator: std.mem.Allocator, json: []const u8) !std.StringHashMap([]const u8) {
    var map = std.StringHashMap([]const u8).init(allocator);

    var i: usize = 0;
    // Skip to first {
    while (i < json.len and json[i] != '{') : (i += 1) {}
    i += 1;

    while (i < json.len) {
        // Skip whitespace
        while (i < json.len and (json[i] == ' ' or json[i] == '\n' or json[i] == '\r' or json[i] == '\t' or json[i] == ',')) : (i += 1) {}
        if (i >= json.len or json[i] == '}') break;

        // Parse key
        if (json[i] != '"') break;
        i += 1;
        const key_start = i;
        while (i < json.len and json[i] != '"') : (i += 1) {}
        const json_key = json[key_start..i];
        i += 1; // skip closing "

        // Skip : and whitespace
        while (i < json.len and (json[i] == ':' or json[i] == ' ')) : (i += 1) {}

        // Parse value
        if (i >= json.len) break;
        if (json[i] == '"') {
            // String value
            i += 1;
            const val_start = i;
            while (i < json.len and json[i] != '"') {
                if (json[i] == '\\') {
                    i += 1; // skip escaped char
                }
                i += 1;
            }
            const val = json[val_start..i];
            i += 1; // skip closing "
            try map.put(json_key, val);
        } else if (json[i] == 'n') {
            // null
            i += 4;
            try map.put(json_key, "");
        } else if (json[i] == 't') {
            i += 4;
            try map.put(json_key, "true");
        } else if (json[i] == 'f') {
            i += 5;
            try map.put(json_key, "false");
        } else {
            // Number
            const val_start = i;
            while (i < json.len and json[i] != ',' and json[i] != '}' and json[i] != ' ' and json[i] != '\n') : (i += 1) {}
            try map.put(json_key, json[val_start..i]);
        }
    }

    return map;
}