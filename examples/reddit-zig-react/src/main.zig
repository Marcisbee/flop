const std = @import("std");
const flop = @import("flop");

const Database = flop.Database.Database;
const Table = flop.Table;
const Auth = flop.Auth;
const Encoding = flop.Encoding;

const Allocator = std.mem.Allocator;

// ===== Schema Definitions =====

const user_schema = Table.Schema{
    .name = "users",
    .fields = &.{
        .{ .name = "email", .field_type = .string, .required = true, .unique = true, .max_len = 255 },
        .{ .name = "password", .field_type = .string, .required = true },
        .{ .name = "handle", .field_type = .string, .required = true, .unique = true, .max_len = 30 },
        .{ .name = "display_name", .field_type = .string, .max_len = 50 },
        .{ .name = "bio", .field_type = .string, .max_len = 500 },
        .{ .name = "avatar", .field_type = .string },
        .{ .name = "karma", .field_type = .int },
    },
    .is_auth = true,
};

const community_schema = Table.Schema{
    .name = "communities",
    .fields = &.{
        .{ .name = "name", .field_type = .string, .required = true, .max_len = 100 },
        .{ .name = "handle", .field_type = .string, .required = true, .unique = true, .max_len = 30 },
        .{ .name = "description", .field_type = .string, .searchable = true, .max_len = 1000 },
        .{ .name = "rules", .field_type = .string, .max_len = 5000 },
        .{ .name = "creator_id", .field_type = .ref, .ref_table = "users", .required = true },
        .{ .name = "member_count", .field_type = .int },
        .{ .name = "visibility", .field_type = .string },
    },
};

const membership_schema = Table.Schema{
    .name = "memberships",
    .fields = &.{
        .{ .name = "user_id", .field_type = .ref, .ref_table = "users", .required = true },
        .{ .name = "community_id", .field_type = .ref, .ref_table = "communities", .required = true },
        .{ .name = "role", .field_type = .string },
    },
};

const post_schema = Table.Schema{
    .name = "posts",
    .fields = &.{
        .{ .name = "title", .field_type = .string, .required = true, .searchable = true, .max_len = 300 },
        .{ .name = "body", .field_type = .string, .searchable = true, .max_len = 40000 },
        .{ .name = "link", .field_type = .string, .max_len = 2048 },
        .{ .name = "image", .field_type = .string },
        .{ .name = "author_id", .field_type = .ref, .ref_table = "users", .required = true },
        .{ .name = "community_id", .field_type = .ref, .ref_table = "communities", .required = true },
        .{ .name = "score", .field_type = .int },
        .{ .name = "hot_rank", .field_type = .float },
        .{ .name = "comment_count", .field_type = .int },
        .{ .name = "repost_of", .field_type = .ref, .ref_table = "posts" },
    },
};

const comment_schema = Table.Schema{
    .name = "comments",
    .fields = &.{
        .{ .name = "body", .field_type = .string, .required = true, .searchable = true, .max_len = 10000 },
        .{ .name = "author_id", .field_type = .ref, .ref_table = "users", .required = true },
        .{ .name = "post_id", .field_type = .ref, .ref_table = "posts", .required = true },
        .{ .name = "parent_id", .field_type = .ref, .ref_table = "comments" },
        .{ .name = "depth", .field_type = .int },
        .{ .name = "path", .field_type = .string },
        .{ .name = "score", .field_type = .int },
    },
};

const vote_schema = Table.Schema{
    .name = "votes",
    .fields = &.{
        .{ .name = "user_id", .field_type = .ref, .ref_table = "users", .required = true },
        .{ .name = "post_id", .field_type = .ref, .ref_table = "posts", .required = true },
        .{ .name = "value", .field_type = .int },
    },
};

const comment_vote_schema = Table.Schema{
    .name = "comment_votes",
    .fields = &.{
        .{ .name = "user_id", .field_type = .ref, .ref_table = "users", .required = true },
        .{ .name = "comment_id", .field_type = .ref, .ref_table = "comments", .required = true },
        .{ .name = "value", .field_type = .int },
    },
};

// ===== Global State =====

var db: Database = undefined;
var auth_mgr: Auth.AuthManager = undefined;
var global_allocator: Allocator = undefined;

// ===== HTTP Request Parsing =====

const HttpRequest = struct {
    method: []const u8,
    path: []const u8,
    query_string: []const u8,
    headers: [64]Header,
    header_count: usize,
    body: []const u8,
    raw_buf: []const u8,

    const Header = struct {
        name: []const u8,
        value: []const u8,
    };

    fn getHeader(self: *const HttpRequest, name: []const u8) ?[]const u8 {
        for (self.headers[0..self.header_count]) |h| {
            if (asciiEqlIgnoreCase(h.name, name)) {
                return h.value;
            }
        }
        return null;
    }
};

fn asciiEqlIgnoreCase(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |ca, cb| {
        if (std.ascii.toLower(ca) != std.ascii.toLower(cb)) return false;
    }
    return true;
}

fn parseHttpRequest(buf: []const u8) ?HttpRequest {
    // Find end of headers
    const header_end = findSubstring(buf, "\r\n\r\n") orelse return null;

    var req = HttpRequest{
        .method = "",
        .path = "",
        .query_string = "",
        .headers = undefined,
        .header_count = 0,
        .body = "",
        .raw_buf = buf,
    };

    // Parse request line
    const header_section = buf[0..header_end];
    const first_line_end = findSubstring(header_section, "\r\n") orelse header_section.len;
    const request_line = header_section[0..first_line_end];

    // METHOD /path HTTP/1.x
    var parts_iter = std.mem.splitScalar(u8, request_line, ' ');
    req.method = parts_iter.first();
    const full_path = parts_iter.next() orelse return null;

    // Split path and query string
    if (std.mem.indexOfScalar(u8, full_path, '?')) |qi| {
        req.path = full_path[0..qi];
        req.query_string = full_path[qi + 1 ..];
    } else {
        req.path = full_path;
        req.query_string = "";
    }

    // Parse headers
    var line_start = first_line_end + 2;
    while (line_start < header_end) {
        const remaining = header_section[line_start..];
        const line_end = findSubstring(remaining, "\r\n") orelse remaining.len;
        const line = remaining[0..line_end];

        if (line.len == 0) break;

        if (std.mem.indexOfScalar(u8, line, ':')) |colon| {
            if (req.header_count < 64) {
                req.headers[req.header_count] = .{
                    .name = std.mem.trim(u8, line[0..colon], " "),
                    .value = std.mem.trim(u8, line[colon + 1 ..], " "),
                };
                req.header_count += 1;
            }
        }

        line_start += line_end + 2;
    }

    // Body
    const body_start = header_end + 4;
    if (body_start < buf.len) {
        // Check Content-Length
        if (req.getHeader("Content-Length")) |cl| {
            const content_len = std.fmt.parseInt(usize, cl, 10) catch 0;
            const available = buf.len - body_start;
            req.body = buf[body_start..body_start + @min(content_len, available)];
        } else {
            req.body = buf[body_start..];
        }
    }

    return req;
}

fn findSubstring(haystack: []const u8, needle: []const u8) ?usize {
    if (needle.len > haystack.len) return null;
    if (needle.len == 0) return 0;
    var i: usize = 0;
    while (i + needle.len <= haystack.len) : (i += 1) {
        if (std.mem.eql(u8, haystack[i .. i + needle.len], needle)) {
            return i;
        }
    }
    return null;
}

// ===== HTTP Response Writing =====

fn writeResponse(stream: std.net.Stream, status: u16, status_text: []const u8, content_type: []const u8, body_data: []const u8) void {
    var header_buf: [1024]u8 = undefined;
    const header = std.fmt.bufPrint(&header_buf, "HTTP/1.1 {d} {s}\r\nContent-Type: {s}\r\nContent-Length: {d}\r\nAccess-Control-Allow-Origin: *\r\nAccess-Control-Allow-Methods: GET, POST, DELETE, OPTIONS\r\nAccess-Control-Allow-Headers: Content-Type, Authorization\r\nConnection: close\r\n\r\n", .{ status, status_text, content_type, body_data.len }) catch return;
    _ = stream.write(header) catch return;
    if (body_data.len > 0) {
        _ = stream.write(body_data) catch return;
    }
}

fn writeJson(stream: std.net.Stream, status: u16, json_body: []const u8) void {
    const status_text: []const u8 = switch (status) {
        200 => "OK",
        201 => "Created",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        409 => "Conflict",
        500 => "Internal Server Error",
        else => "OK",
    };
    writeResponse(stream, status, status_text, "application/json", json_body);
}

fn writeErrorJson(stream: std.net.Stream, status: u16, msg: []const u8) void {
    var buf: [512]u8 = undefined;
    const json = std.fmt.bufPrint(&buf, "{{\"error\":\"{s}\"}}", .{msg}) catch return;
    writeJson(stream, status, json);
}

// ===== JSON Helpers =====

fn getJsonString(json_src: []const u8, key: []const u8) ?[]const u8 {
    // Simple key search: "key"  :  "value" or "key":value
    const search_key = buildSearchKey(key) orelse return null;
    const key_pos = findSubstring(json_src, search_key) orelse return null;
    var pos = key_pos + search_key.len;

    // Skip whitespace and colon
    while (pos < json_src.len and (json_src[pos] == ' ' or json_src[pos] == ':' or json_src[pos] == '\t' or json_src[pos] == '\n' or json_src[pos] == '\r')) : (pos += 1) {}

    if (pos >= json_src.len) return null;

    if (json_src[pos] == '"') {
        // String value
        pos += 1;
        const start = pos;
        while (pos < json_src.len and json_src[pos] != '"') {
            if (json_src[pos] == '\\') pos += 1;
            pos += 1;
        }
        return json_src[start..pos];
    } else if (json_src[pos] == 'n') {
        return null; // null
    } else {
        // Number or boolean
        const start = pos;
        while (pos < json_src.len and json_src[pos] != ',' and json_src[pos] != '}' and json_src[pos] != ' ' and json_src[pos] != '\n') : (pos += 1) {}
        return json_src[start..pos];
    }
}

var search_key_buf: [256]u8 = undefined;

fn buildSearchKey(key: []const u8) ?[]const u8 {
    if (key.len + 2 > 256) return null;
    search_key_buf[0] = '"';
    @memcpy(search_key_buf[1 .. 1 + key.len], key);
    search_key_buf[1 + key.len] = '"';
    return search_key_buf[0 .. key.len + 2];
}

fn parseUint64(s: []const u8) u64 {
    var n: u64 = 0;
    for (s) |c| {
        if (c >= '0' and c <= '9') {
            n = n * 10 + @as(u64, c - '0');
        }
    }
    return n;
}

fn parseInt64(s: []const u8) i64 {
    if (s.len == 0) return 0;
    var neg = false;
    var start: usize = 0;
    if (s[0] == '-') {
        neg = true;
        start = 1;
    }
    var n: i64 = 0;
    for (s[start..]) |c| {
        if (c >= '0' and c <= '9') {
            n = n * 10 + @as(i64, c - '0');
        }
    }
    return if (neg) -n else n;
}

fn getQueryParam(qs: []const u8, name: []const u8) ?[]const u8 {
    var iter = std.mem.splitScalar(u8, qs, '&');
    while (iter.next()) |param| {
        if (std.mem.indexOfScalar(u8, param, '=')) |eq| {
            if (std.mem.eql(u8, param[0..eq], name)) {
                return param[eq + 1 ..];
            }
        }
    }
    return null;
}

// ===== Row JSON Serialization =====

fn getRowStringValue(row: *const Table.Row, key: []const u8) []const u8 {
    if (row.data.get(key)) |val| {
        switch (val) {
            .string => |s| return s,
            else => {},
        }
    }
    return "";
}

fn getRowIntValue(row: *const Table.Row, key: []const u8) i64 {
    if (row.data.get(key)) |val| {
        switch (val) {
            .int => |v| return v,
            .uint => |v| return @intCast(v),
            .string => |s| {
                // Values read back from storage are stored as strings with binary encoding
                if (s.len == 8) {
                    return Encoding.decodeI64(s[0..8]);
                }
                return parseInt64(s);
            },
            else => {},
        }
    }
    return 0;
}

fn getRowUintValue(row: *const Table.Row, key: []const u8) u64 {
    if (row.data.get(key)) |val| {
        switch (val) {
            .uint => |v| return v,
            .int => |v| return @intCast(v),
            .string => |s| {
                if (s.len == 8) {
                    return Encoding.decodeU64(s[0..8]);
                }
                return parseUint64(s);
            },
            else => {},
        }
    }
    return 0;
}

fn getRowFloatValue(row: *const Table.Row, key: []const u8) f64 {
    if (row.data.get(key)) |val| {
        switch (val) {
            .float => |v| return v,
            .int => |v| return @floatFromInt(v),
            .string => |s| {
                if (s.len == 8) {
                    return Encoding.decodeF64(s[0..8]);
                }
                return std.fmt.parseFloat(f64, s) catch 0.0;
            },
            else => {},
        }
    }
    return 0.0;
}

// Build a JSON representation of a Row in the go2 "RawRow" format
fn rowToJson(allocator: Allocator, row: *const Table.Row, schema: *const Table.Schema) ![]u8 {
    var buf = std.array_list.Managed(u8).init(allocator);
    errdefer buf.deinit();

    try buf.appendSlice("{\"ID\":");
    try appendU64(&buf, row.id);
    try buf.appendSlice(",\"TableID\":0,\"Data\":{");

    var first = true;
    for (schema.fields) |field| {
        if (std.mem.eql(u8, field.name, "password")) continue;

        if (!first) try buf.append(',');
        first = false;

        try buf.append('"');
        try buf.appendSlice(field.name);
        try buf.appendSlice("\":");

        switch (field.field_type) {
            .string => {
                const s = getRowStringValue(row, field.name);
                try appendJsonString(&buf, s);
            },
            .int => {
                const v = getRowIntValue(row, field.name);
                try appendI64(&buf, v);
            },
            .float => {
                const v = getRowFloatValue(row, field.name);
                try appendF64(&buf, v);
            },
            .ref => {
                const v = getRowUintValue(row, field.name);
                if (v == 0) {
                    try buf.appendSlice("null");
                } else {
                    try appendU64(&buf, v);
                }
            },
            .boolean => {
                const v = getRowIntValue(row, field.name);
                try buf.appendSlice(if (v != 0) "true" else "false");
            },
            else => {
                const s = getRowStringValue(row, field.name);
                if (s.len > 0) {
                    try appendJsonString(&buf, s);
                } else {
                    try buf.appendSlice("null");
                }
            },
        }
    }

    try buf.appendSlice("},\"CreatedAt\":\"");
    try appendIsoTimestamp(&buf, row.created_at);
    try buf.appendSlice("\",\"UpdatedAt\":\"");
    try appendIsoTimestamp(&buf, row.updated_at);
    try buf.appendSlice("\",\"Version\":");
    try appendU64(&buf, row.version);
    try buf.append('}');

    return buf.toOwnedSlice();
}

// Build a Row JSON with ref includes (e.g., _ref_author_id)
fn rowToJsonWithIncludes(allocator: Allocator, row: *const Table.Row, schema: *const Table.Schema, includes: []const []const u8) ![]u8 {
    var buf = std.array_list.Managed(u8).init(allocator);
    errdefer buf.deinit();

    try buf.appendSlice("{\"ID\":");
    try appendU64(&buf, row.id);
    try buf.appendSlice(",\"TableID\":0,\"Data\":{");

    var first = true;
    for (schema.fields) |field| {
        if (std.mem.eql(u8, field.name, "password")) continue;

        if (!first) try buf.append(',');
        first = false;

        try buf.append('"');
        try buf.appendSlice(field.name);
        try buf.appendSlice("\":");

        switch (field.field_type) {
            .string => {
                const s = getRowStringValue(row, field.name);
                try appendJsonString(&buf, s);
            },
            .int => {
                const v = getRowIntValue(row, field.name);
                try appendI64(&buf, v);
            },
            .float => {
                const v = getRowFloatValue(row, field.name);
                try appendF64(&buf, v);
            },
            .ref => {
                const v = getRowUintValue(row, field.name);
                if (v == 0) {
                    try buf.appendSlice("null");
                } else {
                    try appendU64(&buf, v);
                }
            },
            .boolean => {
                const v = getRowIntValue(row, field.name);
                try buf.appendSlice(if (v != 0) "true" else "false");
            },
            else => {
                const s = getRowStringValue(row, field.name);
                if (s.len > 0) {
                    try appendJsonString(&buf, s);
                } else {
                    try buf.appendSlice("null");
                }
            },
        }
    }

    // Add ref includes
    for (includes) |include_field| {
        const ref_id = getRowUintValue(row, include_field);
        if (ref_id == 0) continue;

        // Find the field's ref_table
        var ref_table_name: ?[]const u8 = null;
        for (schema.fields) |field| {
            if (std.mem.eql(u8, field.name, include_field)) {
                ref_table_name = field.ref_table;
                break;
            }
        }

        if (ref_table_name) |tbl_name| {
            const ref_table = db.table(tbl_name) orelse continue;
            const maybe_ref_row = ref_table.get(ref_id) catch null;
            if (maybe_ref_row) |ref_row_copy| {
                var ref_row = ref_row_copy;
                defer ref_row.deinit();

                try buf.appendSlice(",\"_ref_");
                try buf.appendSlice(include_field);
                try buf.appendSlice("\":{\"id\":");
                try appendU64(&buf, ref_row.id);

                // Find ref schema
                const ref_schema = getSchemaByName(tbl_name);
                if (ref_schema) |rs| {
                    for (rs.fields) |rf| {
                        if (std.mem.eql(u8, rf.name, "password")) continue;
                        try buf.appendSlice(",\"");
                        try buf.appendSlice(rf.name);
                        try buf.appendSlice("\":");
                        switch (rf.field_type) {
                            .string => {
                                const sv = getRowStringValue(&ref_row, rf.name);
                                try appendJsonString(&buf, sv);
                            },
                            .int => {
                                const iv = getRowIntValue(&ref_row, rf.name);
                                try appendI64(&buf, iv);
                            },
                            .float => {
                                const fv = getRowFloatValue(&ref_row, rf.name);
                                try appendF64(&buf, fv);
                            },
                            .ref => {
                                const rv = getRowUintValue(&ref_row, rf.name);
                                if (rv == 0) {
                                    try buf.appendSlice("null");
                                } else {
                                    try appendU64(&buf, rv);
                                }
                            },
                            else => {
                                const sv = getRowStringValue(&ref_row, rf.name);
                                if (sv.len > 0) try appendJsonString(&buf, sv) else try buf.appendSlice("null");
                            },
                        }
                    }
                }
                try buf.append('}');
            }
        }
    }

    try buf.appendSlice("},\"CreatedAt\":\"");
    try appendIsoTimestamp(&buf, row.created_at);
    try buf.appendSlice("\",\"UpdatedAt\":\"");
    try appendIsoTimestamp(&buf, row.updated_at);
    try buf.appendSlice("\",\"Version\":");
    try appendU64(&buf, row.version);
    try buf.append('}');

    return buf.toOwnedSlice();
}

fn getSchemaByName(name: []const u8) ?*const Table.Schema {
    if (std.mem.eql(u8, name, "users")) return &user_schema;
    if (std.mem.eql(u8, name, "communities")) return &community_schema;
    if (std.mem.eql(u8, name, "memberships")) return &membership_schema;
    if (std.mem.eql(u8, name, "posts")) return &post_schema;
    if (std.mem.eql(u8, name, "comments")) return &comment_schema;
    if (std.mem.eql(u8, name, "votes")) return &vote_schema;
    if (std.mem.eql(u8, name, "comment_votes")) return &comment_vote_schema;
    return null;
}

fn appendJsonString(buf: *std.array_list.Managed(u8), s: []const u8) !void {
    try buf.append('"');
    for (s) |c| {
        switch (c) {
            '"' => try buf.appendSlice("\\\""),
            '\\' => try buf.appendSlice("\\\\"),
            '\n' => try buf.appendSlice("\\n"),
            '\r' => try buf.appendSlice("\\r"),
            '\t' => try buf.appendSlice("\\t"),
            else => {
                if (c < 0x20) {
                    const hex_chars = "0123456789abcdef";
                    try buf.appendSlice("\\u00");
                    try buf.append(hex_chars[c >> 4]);
                    try buf.append(hex_chars[c & 0xf]);
                } else {
                    try buf.append(c);
                }
            },
        }
    }
    try buf.append('"');
}

fn appendU64(buf: *std.array_list.Managed(u8), v: u64) !void {
    var num_buf: [32]u8 = undefined;
    const s = std.fmt.bufPrint(&num_buf, "{d}", .{v}) catch unreachable;
    try buf.appendSlice(s);
}

fn appendI64(buf: *std.array_list.Managed(u8), v: i64) !void {
    var num_buf: [32]u8 = undefined;
    const s = std.fmt.bufPrint(&num_buf, "{d}", .{v}) catch unreachable;
    try buf.appendSlice(s);
}

fn appendF64(buf: *std.array_list.Managed(u8), v: f64) !void {
    var num_buf: [64]u8 = undefined;
    const s = std.fmt.bufPrint(&num_buf, "{d}", .{v}) catch unreachable;
    try buf.appendSlice(s);
}

fn appendIsoTimestamp(buf: *std.array_list.Managed(u8), millis: i64) !void {
    // Convert millis to a basic ISO timestamp
    const epoch_secs: i64 = @divTrunc(millis, 1000);
    const ms_part: u64 = @intCast(@mod(millis, 1000));

    // Use Zig's epoch seconds conversion
    const es: std.time.epoch.EpochSeconds = .{ .secs = @intCast(epoch_secs) };
    const day_seconds = es.getDaySeconds();
    const year_day = es.getEpochDay().calculateYearDay();
    const month_day = year_day.calculateMonthDay();

    var ts_buf: [32]u8 = undefined;
    const ts = std.fmt.bufPrint(&ts_buf, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}.{d:0>3}Z", .{
        year_day.year,
        month_day.month.numeric(),
        month_day.day_index + 1,
        day_seconds.getHoursIntoDay(),
        day_seconds.getMinutesIntoHour(),
        day_seconds.getSecondsIntoMinute(),
        ms_part,
    }) catch "1970-01-01T00:00:00.000Z";
    try buf.appendSlice(ts);
}

// ===== Authentication Helper =====

fn authenticateRequest(req: *const HttpRequest) ?u64 {
    const auth_header = req.getHeader("Authorization") orelse return null;
    if (auth_header.len < 8) return null;
    // "Bearer <token>"
    if (!std.mem.startsWith(u8, auth_header, "Bearer ")) return null;
    const token = auth_header[7..];
    return auth_mgr.validateToken(token);
}

// ===== Hot Rank Calculation =====

fn hotRank(score: i64, created_at_millis: i64) f64 {
    const abs_score_f: f64 = @floatFromInt(if (score < 0) -score else score);
    const order = @log10(@max(abs_score_f, 1.0));
    var sign: f64 = 0.0;
    if (score > 0) {
        sign = 1.0;
    } else if (score < 0) {
        sign = -1.0;
    }
    const epoch_secs_f: f64 = @floatFromInt(@divTrunc(created_at_millis, 1000));
    const seconds = (epoch_secs_f - 1134028003.0) / 45000.0;
    return sign * order + seconds;
}

// ===== Scan-based helpers using function pointer callbacks =====

const ScanContext = struct {
    results: *std.array_list.Managed(Table.Row),
    allocator: Allocator,
};

const FieldFilterContext = struct {
    results: *std.array_list.Managed(Table.Row),
    allocator: Allocator,
    field_name: []const u8,
    match_value: u64,
};

const StringFilterContext = struct {
    results: *std.array_list.Managed(Table.Row),
    allocator: Allocator,
    field_name: []const u8,
    match_value: []const u8,
};

const TwoFieldFilterContext = struct {
    results: *std.array_list.Managed(Table.Row),
    allocator: Allocator,
    field1_name: []const u8,
    field1_value: u64,
    field2_name: []const u8,
    field2_value: u64,
    found: bool,
};

fn collectAllRows(allocator: Allocator, tbl: *Table.Table) !std.array_list.Managed(Table.Row) {
    var results = std.array_list.Managed(Table.Row).init(allocator);
    var page_id = tbl.pager.root_page_id;

    // We need to access pager directly since scan() requires fn pointers
    while (page_id != 0) {
        const page = try tbl.pager.readPage(page_id);
        const n = page.numEntries();
        var i: u16 = 0;
        while (i < n) : (i += 1) {
            if (page.entryAt(i)) |entry| {
                if (entry.value.len >= Encoding.ROW_HEADER_SIZE) {
                    const row = try decodeRowFromRaw(allocator, entry.value);
                    try results.append(row);
                }
            }
        }
        page_id = page.overflowId();
    }

    return results;
}

fn decodeRowFromRaw(allocator: Allocator, raw: []const u8) !Table.Row {
    if (raw.len < Encoding.ROW_HEADER_SIZE) return error.InvalidData;

    var row = Table.Row.init(allocator);
    row.id = Encoding.decodeU64(raw[0..8]);
    row.created_at = Encoding.decodeI64(raw[8..16]);
    row.updated_at = Encoding.decodeI64(raw[16..24]);
    row.version = Encoding.decodeU64(raw[24..32]);

    var offset: usize = Encoding.ROW_HEADER_SIZE;
    while (offset + 6 <= raw.len) {
        const name_len = std.mem.readInt(u16, raw[offset..][0..2], .little);
        offset += 2;
        if (offset + name_len + 4 > raw.len) break;
        const name = raw[offset .. offset + name_len];
        offset += name_len;
        const data_len = std.mem.readInt(u32, raw[offset..][0..4], .little);
        offset += 4;
        if (offset + data_len > raw.len) break;
        const field_data = raw[offset .. offset + data_len];
        offset += data_len;

        try row.put(name, Table.Value{ .string = field_data });
    }
    return row;
}

fn deinitRowList(list: *std.array_list.Managed(Table.Row)) void {
    for (list.items) |*r| {
        r.deinit();
    }
    list.deinit();
}

// ===== View Response Builder =====

fn buildViewResponse(allocator: Allocator, rows: []const Table.Row, total: usize, schema: *const Table.Schema, includes: []const []const u8) ![]u8 {
    var buf = std.array_list.Managed(u8).init(allocator);
    errdefer buf.deinit();

    try buf.appendSlice("{\"data\":[");

    for (rows, 0..) |*row, idx| {
        if (idx > 0) try buf.append(',');
        if (includes.len > 0) {
            const row_json = try rowToJsonWithIncludes(allocator, row, schema, includes);
            defer allocator.free(row_json);
            try buf.appendSlice(row_json);
        } else {
            const row_json = try rowToJson(allocator, row, schema);
            defer allocator.free(row_json);
            try buf.appendSlice(row_json);
        }
    }

    try buf.appendSlice("],\"total\":");
    try appendU64(&buf, @intCast(total));
    try buf.append('}');

    return buf.toOwnedSlice();
}

// ===== Route Handling =====

fn handleRequest(allocator: Allocator, req: *const HttpRequest, stream: std.net.Stream) void {
    // CORS preflight
    if (std.mem.eql(u8, req.method, "OPTIONS")) {
        writeResponse(stream, 200, "OK", "text/plain", "");
        return;
    }

    const path = req.path;

    // API Routes
    if (std.mem.startsWith(u8, path, "/api/")) {
        handleApiRoute(allocator, req, stream) catch |err| {
            std.log.err("API error: {}", .{err});
            writeErrorJson(stream, 500, "internal server error");
        };
        return;
    }

    // Static files
    if (std.mem.startsWith(u8, path, "/static/")) {
        handleStaticFile(stream, path[8..]);
        return;
    }

    // SPA fallback
    serveSpaHtml(stream);
}

fn handleApiRoute(allocator: Allocator, req: *const HttpRequest, stream: std.net.Stream) !void {
    const path = req.path;
    const method = req.method;

    // ===== AUTH ROUTES =====

    if (std.mem.eql(u8, path, "/api/auth/register") and std.mem.eql(u8, method, "POST")) {
        return handleRegister(allocator, req, stream);
    }

    if (std.mem.eql(u8, path, "/api/auth/login") and std.mem.eql(u8, method, "POST")) {
        return handleLogin(allocator, req, stream);
    }

    if (std.mem.eql(u8, path, "/api/auth/refresh") and std.mem.eql(u8, method, "POST")) {
        return handleRefresh(allocator, req, stream);
    }

    if (std.mem.eql(u8, path, "/api/auth/me") and std.mem.eql(u8, method, "GET")) {
        return handleMe(req, stream);
    }

    // ===== COMMUNITY ROUTES =====

    if (std.mem.eql(u8, path, "/api/communities") and std.mem.eql(u8, method, "GET")) {
        return handleListCommunities(allocator, stream);
    }

    if (std.mem.eql(u8, path, "/api/communities") and std.mem.eql(u8, method, "POST")) {
        return handleCreateCommunity(allocator, req, stream);
    }

    // /api/communities/{id}/toggle_join
    if (std.mem.startsWith(u8, path, "/api/communities/") and std.mem.endsWith(u8, path, "/toggle_join") and std.mem.eql(u8, method, "POST")) {
        const id_part = path[17 .. path.len - 12];
        const community_id = parseUint64(id_part);
        return handleToggleJoin(allocator, req, stream, community_id);
    }

    // /api/communities/{id}/membership
    if (std.mem.startsWith(u8, path, "/api/communities/") and std.mem.endsWith(u8, path, "/membership") and std.mem.eql(u8, method, "GET")) {
        const id_part = path[17 .. path.len - 11];
        const community_id = parseUint64(id_part);
        return handleCheckMembership(allocator, req, stream, community_id);
    }

    // /api/communities/{handle} (GET) - handle is a string, not numeric ID
    if (std.mem.startsWith(u8, path, "/api/communities/") and std.mem.eql(u8, method, "GET")) {
        const handle = path[17..];
        if (handle.len > 0 and !std.mem.eql(u8, handle, "")) {
            return handleGetCommunity(allocator, stream, handle);
        }
    }

    // ===== FEED ROUTES =====

    if (std.mem.eql(u8, path, "/api/feed/hot") and std.mem.eql(u8, method, "GET")) {
        return handleFeed(allocator, req, stream, .hot);
    }

    if (std.mem.eql(u8, path, "/api/feed/new") and std.mem.eql(u8, method, "GET")) {
        return handleFeed(allocator, req, stream, .new);
    }

    if (std.mem.eql(u8, path, "/api/feed/best") and std.mem.eql(u8, method, "GET")) {
        return handleFeed(allocator, req, stream, .best);
    }

    // ===== POST ROUTES =====

    // /api/c/{id}/posts
    if (std.mem.startsWith(u8, path, "/api/c/") and std.mem.endsWith(u8, path, "/posts") and std.mem.eql(u8, method, "GET")) {
        const id_str = path[7 .. path.len - 6];
        const community_id = parseUint64(id_str);
        return handleCommunityPosts(allocator, stream, community_id);
    }

    // /api/posts/{id}/vote
    if (std.mem.startsWith(u8, path, "/api/posts/") and std.mem.endsWith(u8, path, "/vote") and std.mem.eql(u8, method, "POST")) {
        const id_str = path[11 .. path.len - 5];
        const post_id = parseUint64(id_str);
        return handlePostVote(allocator, req, stream, post_id);
    }

    // /api/posts/{id}/comments
    if (std.mem.startsWith(u8, path, "/api/posts/") and std.mem.endsWith(u8, path, "/comments")) {
        const id_str = path[11 .. path.len - 9];
        const post_id = parseUint64(id_str);
        if (std.mem.eql(u8, method, "GET")) {
            return handleGetComments(allocator, stream, post_id);
        }
        if (std.mem.eql(u8, method, "POST")) {
            return handleCreateComment(allocator, req, stream, post_id);
        }
    }

    // /api/posts (POST - create)
    if (std.mem.eql(u8, path, "/api/posts") and std.mem.eql(u8, method, "POST")) {
        return handleCreatePost(allocator, req, stream);
    }

    // /api/posts/{id} (GET)
    if (std.mem.startsWith(u8, path, "/api/posts/") and std.mem.eql(u8, method, "GET")) {
        const id_str = path[11..];
        const post_id = parseUint64(id_str);
        return handleGetPost(allocator, stream, post_id);
    }

    // ===== SEARCH =====
    if (std.mem.eql(u8, path, "/api/search/posts") and std.mem.eql(u8, method, "GET")) {
        return handleSearchPosts(allocator, req, stream);
    }

    // ===== USER POSTS =====
    // /api/users/{id}/posts
    if (std.mem.startsWith(u8, path, "/api/users/") and std.mem.endsWith(u8, path, "/posts") and std.mem.eql(u8, method, "GET")) {
        const id_str = path[11 .. path.len - 6];
        const author_id = parseUint64(id_str);
        return handleUserPosts(allocator, stream, author_id);
    }

    // ===== COMMENT VOTES =====
    // /api/comments/{id}/vote
    if (std.mem.startsWith(u8, path, "/api/comments/") and std.mem.endsWith(u8, path, "/vote") and std.mem.eql(u8, method, "POST")) {
        const id_str = path[14 .. path.len - 5];
        const comment_id = parseUint64(id_str);
        return handleCommentVote(allocator, req, stream, comment_id);
    }

    writeErrorJson(stream, 404, "not found");
}

// ===== Auth Handlers =====

fn handleRegister(allocator: Allocator, req: *const HttpRequest, stream: std.net.Stream) void {
    const body = req.body;

    const email = getJsonString(body, "email") orelse {
        writeErrorJson(stream, 400, "email is required");
        return;
    };
    const password = getJsonString(body, "password") orelse {
        writeErrorJson(stream, 400, "password is required");
        return;
    };
    const handle = getJsonString(body, "handle") orelse {
        writeErrorJson(stream, 400, "handle is required");
        return;
    };
    const display_name = getJsonString(body, "display_name") orelse handle;

    if (email.len == 0 or password.len == 0 or handle.len == 0) {
        writeErrorJson(stream, 400, "email, password, and handle are required");
        return;
    }

    // Hash the password
    const pw_hash = Auth.AuthManager.sha256Hex(password);
    const pw_hash_slice: []const u8 = &pw_hash;

    // Insert user
    var data = std.StringHashMap(Table.Value).init(allocator);
    defer data.deinit();
    data.put("email", Table.Value{ .string = email }) catch return;
    data.put("password", Table.Value{ .string = pw_hash_slice }) catch return;
    data.put("handle", Table.Value{ .string = handle }) catch return;
    data.put("display_name", Table.Value{ .string = display_name }) catch return;
    data.put("bio", Table.Value{ .string = "" }) catch return;
    data.put("avatar", Table.Value{ .string = "" }) catch return;
    data.put("karma", Table.Value{ .int = 0 }) catch return;

    const user = db.insert("users", &data) catch {
        writeErrorJson(stream, 409, "user already exists");
        return;
    };

    // Create token
    const token = auth_mgr.makeToken(user.id) catch {
        writeErrorJson(stream, 500, "failed to create token");
        return;
    };

    // Build response
    var buf = std.array_list.Managed(u8).init(allocator);
    defer buf.deinit();
    buf.appendSlice("{\"user\":{\"id\":") catch return;
    appendU64(&buf, user.id) catch return;
    buf.appendSlice(",\"email\":") catch return;
    appendJsonString(&buf, email) catch return;
    buf.appendSlice(",\"handle\":") catch return;
    appendJsonString(&buf, handle) catch return;
    buf.appendSlice(",\"display_name\":") catch return;
    appendJsonString(&buf, display_name) catch return;
    buf.appendSlice(",\"bio\":\"\",\"avatar\":\"\",\"karma\":0},\"token\":") catch return;
    appendJsonString(&buf, token) catch return;
    buf.append('}') catch return;

    writeJson(stream, 200, buf.items);
}

fn handleLogin(allocator: Allocator, req: *const HttpRequest, stream: std.net.Stream) void {
    const body = req.body;

    const email = getJsonString(body, "email") orelse {
        writeErrorJson(stream, 400, "email required");
        return;
    };
    const password = getJsonString(body, "password") orelse {
        writeErrorJson(stream, 400, "password required");
        return;
    };

    const pw_hash = Auth.AuthManager.sha256Hex(password);
    const pw_hash_slice: []const u8 = &pw_hash;

    // Find user by email
    const users_tbl = db.table("users") orelse {
        writeErrorJson(stream, 500, "users table not found");
        return;
    };

    var all_rows = collectAllRows(allocator, users_tbl) catch {
        writeErrorJson(stream, 500, "scan error");
        return;
    };
    defer deinitRowList(&all_rows);

    var found_user: ?*Table.Row = null;
    for (all_rows.items) |*row| {
        const row_email = getRowStringValue(row, "email");
        if (std.mem.eql(u8, row_email, email)) {
            const row_pw = getRowStringValue(row, "password");
            if (std.mem.eql(u8, row_pw, pw_hash_slice)) {
                found_user = row;
                break;
            }
        }
    }

    const user_row = found_user orelse {
        writeErrorJson(stream, 401, "invalid credentials");
        return;
    };

    const token = auth_mgr.makeToken(user_row.id) catch {
        writeErrorJson(stream, 500, "failed to create token");
        return;
    };

    // Build response
    var buf = std.array_list.Managed(u8).init(allocator);
    defer buf.deinit();
    buf.appendSlice("{\"user\":{\"id\":") catch return;
    appendU64(&buf, user_row.id) catch return;
    buf.appendSlice(",\"email\":") catch return;
    appendJsonString(&buf, getRowStringValue(user_row, "email")) catch return;
    buf.appendSlice(",\"handle\":") catch return;
    appendJsonString(&buf, getRowStringValue(user_row, "handle")) catch return;
    buf.appendSlice(",\"display_name\":") catch return;
    appendJsonString(&buf, getRowStringValue(user_row, "display_name")) catch return;
    buf.appendSlice(",\"bio\":") catch return;
    appendJsonString(&buf, getRowStringValue(user_row, "bio")) catch return;
    buf.appendSlice(",\"avatar\":") catch return;
    appendJsonString(&buf, getRowStringValue(user_row, "avatar")) catch return;
    buf.appendSlice(",\"karma\":") catch return;
    appendI64(&buf, getRowIntValue(user_row, "karma")) catch return;
    buf.appendSlice("},\"token\":") catch return;
    appendJsonString(&buf, token) catch return;
    buf.append('}') catch return;

    writeJson(stream, 200, buf.items);
}

fn handleRefresh(_: Allocator, _: *const HttpRequest, stream: std.net.Stream) void {
    // Simplified: refresh not fully implemented, return error
    writeErrorJson(stream, 401, "refresh not supported in zig backend");
}

fn handleMe(req: *const HttpRequest, stream: std.net.Stream) void {
    const user_id = authenticateRequest(req) orelse {
        writeErrorJson(stream, 401, "unauthorized");
        return;
    };

    const users_tbl = db.table("users") orelse {
        writeErrorJson(stream, 500, "users table not found");
        return;
    };

    const maybe_user = users_tbl.get(user_id) catch {
        writeErrorJson(stream, 500, "error fetching user");
        return;
    };

    if (maybe_user == null) {
        writeErrorJson(stream, 401, "user not found");
        return;
    }

    var user = maybe_user.?;
    defer user.deinit();

    var buf = std.array_list.Managed(u8).init(global_allocator);
    defer buf.deinit();
    buf.appendSlice("{\"user\":{\"id\":") catch return;
    appendU64(&buf, user.id) catch return;
    buf.appendSlice(",\"email\":") catch return;
    appendJsonString(&buf, getRowStringValue(&user, "email")) catch return;
    buf.appendSlice(",\"handle\":") catch return;
    appendJsonString(&buf, getRowStringValue(&user, "handle")) catch return;
    buf.appendSlice(",\"display_name\":") catch return;
    appendJsonString(&buf, getRowStringValue(&user, "display_name")) catch return;
    buf.appendSlice(",\"bio\":") catch return;
    appendJsonString(&buf, getRowStringValue(&user, "bio")) catch return;
    buf.appendSlice(",\"avatar\":") catch return;
    appendJsonString(&buf, getRowStringValue(&user, "avatar")) catch return;
    buf.appendSlice(",\"karma\":") catch return;
    appendI64(&buf, getRowIntValue(&user, "karma")) catch return;
    buf.appendSlice("}}") catch return;

    writeJson(stream, 200, buf.items);
}

// ===== Community Handlers =====

fn handleListCommunities(allocator: Allocator, stream: std.net.Stream) void {
    const tbl = db.table("communities") orelse {
        writeJson(stream, 200, "{\"data\":[],\"total\":0}");
        return;
    };

    var rows = collectAllRows(allocator, tbl) catch {
        writeJson(stream, 200, "{\"data\":[],\"total\":0}");
        return;
    };
    defer deinitRowList(&rows);

    // Sort by member_count desc
    std.mem.sortUnstable(Table.Row, rows.items, {}, struct {
        fn cmp(_: void, a: Table.Row, b: Table.Row) bool {
            return getRowIntValue(&a, "member_count") > getRowIntValue(&b, "member_count");
        }
    }.cmp);

    const limit: usize = @min(50, rows.items.len);
    const resp = buildViewResponse(allocator, rows.items[0..limit], rows.items.len, &community_schema, &.{}) catch {
        writeJson(stream, 200, "{\"data\":[],\"total\":0}");
        return;
    };
    defer allocator.free(resp);
    writeJson(stream, 200, resp);
}

fn handleGetCommunity(allocator: Allocator, stream: std.net.Stream, handle: []const u8) void {
    const tbl = db.table("communities") orelse {
        writeJson(stream, 200, "{\"data\":[],\"total\":0}");
        return;
    };

    var rows = collectAllRows(allocator, tbl) catch {
        writeJson(stream, 200, "{\"data\":[],\"total\":0}");
        return;
    };
    defer deinitRowList(&rows);

    // Find by handle
    var found_rows = std.array_list.Managed(Table.Row).init(allocator);
    defer found_rows.deinit();

    for (rows.items) |row| {
        const row_handle = getRowStringValue(&row, "handle");
        if (std.mem.eql(u8, row_handle, handle)) {
            found_rows.append(row) catch continue;
            break;
        }
    }

    const includes: []const []const u8 = &.{"creator_id"};
    const resp = buildViewResponse(allocator, found_rows.items, found_rows.items.len, &community_schema, includes) catch {
        writeJson(stream, 200, "{\"data\":[],\"total\":0}");
        return;
    };
    defer allocator.free(resp);
    writeJson(stream, 200, resp);
}

fn handleCreateCommunity(allocator: Allocator, req: *const HttpRequest, stream: std.net.Stream) void {
    const user_id = authenticateRequest(req) orelse {
        writeErrorJson(stream, 401, "unauthorized");
        return;
    };

    const body = req.body;
    const name = getJsonString(body, "name") orelse {
        writeErrorJson(stream, 400, "name is required");
        return;
    };
    const handle = getJsonString(body, "handle") orelse {
        writeErrorJson(stream, 400, "handle is required");
        return;
    };
    const description = getJsonString(body, "description") orelse "";

    if (name.len == 0 or handle.len == 0) {
        writeErrorJson(stream, 400, "name and handle are required");
        return;
    }

    var data = std.StringHashMap(Table.Value).init(allocator);
    defer data.deinit();
    data.put("name", Table.Value{ .string = name }) catch return;
    data.put("handle", Table.Value{ .string = handle }) catch return;
    data.put("description", Table.Value{ .string = description }) catch return;
    data.put("rules", Table.Value{ .string = "" }) catch return;
    data.put("creator_id", Table.Value{ .uint = user_id }) catch return;
    data.put("member_count", Table.Value{ .int = 1 }) catch return;
    data.put("visibility", Table.Value{ .string = "public" }) catch return;

    const community = db.insert("communities", &data) catch {
        writeErrorJson(stream, 409, "community already exists");
        return;
    };

    // Auto-join creator
    var mem_data = std.StringHashMap(Table.Value).init(allocator);
    defer mem_data.deinit();
    mem_data.put("user_id", Table.Value{ .uint = user_id }) catch return;
    mem_data.put("community_id", Table.Value{ .uint = community.id }) catch return;
    mem_data.put("role", Table.Value{ .string = "admin" }) catch return;
    _ = db.insert("memberships", &mem_data) catch {};

    const row_json = rowToJson(allocator, &community, &community_schema) catch return;
    defer allocator.free(row_json);

    var buf = std.array_list.Managed(u8).init(allocator);
    defer buf.deinit();
    buf.appendSlice("{\"community\":") catch return;
    buf.appendSlice(row_json) catch return;
    buf.append('}') catch return;

    writeJson(stream, 200, buf.items);
}

fn handleToggleJoin(allocator: Allocator, req: *const HttpRequest, stream: std.net.Stream, community_id: u64) void {
    const user_id = authenticateRequest(req) orelse {
        writeErrorJson(stream, 401, "unauthorized");
        return;
    };

    const mem_tbl = db.table("memberships") orelse {
        writeErrorJson(stream, 500, "memberships table not found");
        return;
    };

    var all_mems = collectAllRows(allocator, mem_tbl) catch {
        writeErrorJson(stream, 500, "scan error");
        return;
    };
    defer deinitRowList(&all_mems);

    // Find existing membership
    var existing_id: ?u64 = null;
    for (all_mems.items) |*row| {
        const row_user = getRowUintValue(row, "user_id");
        const row_comm = getRowUintValue(row, "community_id");
        if (row_user == user_id and row_comm == community_id) {
            existing_id = row.id;
            break;
        }
    }

    if (existing_id) |eid| {
        // Leave
        _ = db.deleteRow("memberships", eid) catch {};

        // Decrement member_count
        const comm_tbl = db.table("communities") orelse return;
        const maybe_comm = comm_tbl.get(community_id) catch null;
        if (maybe_comm) |comm_copy| {
            var comm_row = comm_copy;
            defer comm_row.deinit();
            var count = getRowIntValue(&comm_row, "member_count") - 1;
            if (count < 0) count = 0;
            var upd = std.StringHashMap(Table.Value).init(allocator);
            defer upd.deinit();
            upd.put("member_count", Table.Value{ .int = count }) catch return;
            _ = db.update("communities", community_id, &upd) catch {};
        }

        writeJson(stream, 200, "{\"joined\":false}");
    } else {
        // Join
        var mem_data = std.StringHashMap(Table.Value).init(allocator);
        defer mem_data.deinit();
        mem_data.put("user_id", Table.Value{ .uint = user_id }) catch return;
        mem_data.put("community_id", Table.Value{ .uint = community_id }) catch return;
        mem_data.put("role", Table.Value{ .string = "member" }) catch return;
        _ = db.insert("memberships", &mem_data) catch {
            writeErrorJson(stream, 500, "failed to join");
            return;
        };

        // Increment member_count
        const comm_tbl = db.table("communities") orelse return;
        const maybe_comm2 = comm_tbl.get(community_id) catch null;
        if (maybe_comm2) |comm_copy2| {
            var comm_row = comm_copy2;
            defer comm_row.deinit();
            const count = getRowIntValue(&comm_row, "member_count") + 1;
            var upd = std.StringHashMap(Table.Value).init(allocator);
            defer upd.deinit();
            upd.put("member_count", Table.Value{ .int = count }) catch return;
            _ = db.update("communities", community_id, &upd) catch {};
        }

        writeJson(stream, 200, "{\"joined\":true}");
    }
}

fn handleCheckMembership(allocator: Allocator, req: *const HttpRequest, stream: std.net.Stream, community_id: u64) void {
    const user_id = authenticateRequest(req) orelse {
        writeJson(stream, 200, "{\"joined\":false}");
        return;
    };

    const mem_tbl = db.table("memberships") orelse {
        writeJson(stream, 200, "{\"joined\":false}");
        return;
    };

    var all_mems = collectAllRows(allocator, mem_tbl) catch {
        writeJson(stream, 200, "{\"joined\":false}");
        return;
    };
    defer deinitRowList(&all_mems);

    for (all_mems.items) |*row| {
        const row_user = getRowUintValue(row, "user_id");
        const row_comm = getRowUintValue(row, "community_id");
        if (row_user == user_id and row_comm == community_id) {
            writeJson(stream, 200, "{\"joined\":true}");
            return;
        }
    }

    writeJson(stream, 200, "{\"joined\":false}");
}

// ===== Feed Handlers =====

const FeedType = enum { hot, new, best };

fn handleFeed(allocator: Allocator, req: *const HttpRequest, stream: std.net.Stream, feed_type: FeedType) void {
    _ = req;
    const tbl = db.table("posts") orelse {
        writeJson(stream, 200, "{\"data\":[],\"total\":0}");
        return;
    };

    var rows = collectAllRows(allocator, tbl) catch {
        writeJson(stream, 200, "{\"data\":[],\"total\":0}");
        return;
    };
    defer deinitRowList(&rows);

    // Sort based on feed type
    switch (feed_type) {
        .hot => {
            std.mem.sortUnstable(Table.Row, rows.items, {}, struct {
                fn cmp(_: void, a: Table.Row, b: Table.Row) bool {
                    return getRowFloatValue(&a, "hot_rank") > getRowFloatValue(&b, "hot_rank");
                }
            }.cmp);
        },
        .new => {
            std.mem.sortUnstable(Table.Row, rows.items, {}, struct {
                fn cmp(_: void, a: Table.Row, b: Table.Row) bool {
                    return a.id > b.id;
                }
            }.cmp);
        },
        .best => {
            std.mem.sortUnstable(Table.Row, rows.items, {}, struct {
                fn cmp(_: void, a: Table.Row, b: Table.Row) bool {
                    return getRowIntValue(&a, "score") > getRowIntValue(&b, "score");
                }
            }.cmp);
        },
    }

    const limit: usize = @min(25, rows.items.len);
    const includes: []const []const u8 = &.{ "author_id", "community_id" };
    const resp = buildViewResponse(allocator, rows.items[0..limit], rows.items.len, &post_schema, includes) catch {
        writeJson(stream, 200, "{\"data\":[],\"total\":0}");
        return;
    };
    defer allocator.free(resp);
    writeJson(stream, 200, resp);
}

// ===== Post Handlers =====

fn handleGetPost(allocator: Allocator, stream: std.net.Stream, post_id: u64) void {
    const tbl = db.table("posts") orelse {
        writeJson(stream, 200, "{\"data\":[],\"total\":0}");
        return;
    };

    const maybe_post = tbl.get(post_id) catch null;
    if (maybe_post) |post_copy| {
        var post = post_copy;
        defer post.deinit();

        const includes: []const []const u8 = &.{ "author_id", "community_id" };
        const row_json = rowToJsonWithIncludes(allocator, &post, &post_schema, includes) catch {
            writeJson(stream, 200, "{\"data\":[],\"total\":0}");
            return;
        };
        defer allocator.free(row_json);

        var buf = std.array_list.Managed(u8).init(allocator);
        defer buf.deinit();
        buf.appendSlice("{\"data\":[") catch return;
        buf.appendSlice(row_json) catch return;
        buf.appendSlice("],\"total\":1}") catch return;
        writeJson(stream, 200, buf.items);
    } else {
        writeJson(stream, 200, "{\"data\":[],\"total\":0}");
    }
}

fn handleCommunityPosts(allocator: Allocator, stream: std.net.Stream, community_id: u64) void {
    const tbl = db.table("posts") orelse {
        writeJson(stream, 200, "{\"data\":[],\"total\":0}");
        return;
    };

    var rows = collectAllRows(allocator, tbl) catch {
        writeJson(stream, 200, "{\"data\":[],\"total\":0}");
        return;
    };
    defer deinitRowList(&rows);

    // Filter by community_id
    var filtered = std.array_list.Managed(Table.Row).init(allocator);
    defer filtered.deinit();

    for (rows.items) |row| {
        const row_comm = getRowUintValue(&row, "community_id");
        if (row_comm == community_id) {
            filtered.append(row) catch continue;
        }
    }

    // Sort by hot_rank desc
    std.mem.sortUnstable(Table.Row, filtered.items, {}, struct {
        fn cmp(_: void, a: Table.Row, b: Table.Row) bool {
            return getRowFloatValue(&a, "hot_rank") > getRowFloatValue(&b, "hot_rank");
        }
    }.cmp);

    const limit: usize = @min(25, filtered.items.len);
    const includes: []const []const u8 = &.{"author_id"};
    const resp = buildViewResponse(allocator, filtered.items[0..limit], filtered.items.len, &post_schema, includes) catch {
        writeJson(stream, 200, "{\"data\":[],\"total\":0}");
        return;
    };
    defer allocator.free(resp);
    writeJson(stream, 200, resp);
}

fn handleUserPosts(allocator: Allocator, stream: std.net.Stream, author_id: u64) void {
    const tbl = db.table("posts") orelse {
        writeJson(stream, 200, "{\"data\":[],\"total\":0}");
        return;
    };

    var rows = collectAllRows(allocator, tbl) catch {
        writeJson(stream, 200, "{\"data\":[],\"total\":0}");
        return;
    };
    defer deinitRowList(&rows);

    // Filter by author_id
    var filtered = std.array_list.Managed(Table.Row).init(allocator);
    defer filtered.deinit();

    for (rows.items) |row| {
        const row_author = getRowUintValue(&row, "author_id");
        if (row_author == author_id) {
            filtered.append(row) catch continue;
        }
    }

    // Sort by id desc (newest first)
    std.mem.sortUnstable(Table.Row, filtered.items, {}, struct {
        fn cmp(_: void, a: Table.Row, b: Table.Row) bool {
            return a.id > b.id;
        }
    }.cmp);

    const limit: usize = @min(25, filtered.items.len);
    const resp = buildViewResponse(allocator, filtered.items[0..limit], filtered.items.len, &post_schema, &.{}) catch {
        writeJson(stream, 200, "{\"data\":[],\"total\":0}");
        return;
    };
    defer allocator.free(resp);
    writeJson(stream, 200, resp);
}

fn handleSearchPosts(allocator: Allocator, req: *const HttpRequest, stream: std.net.Stream) void {
    const query = getQueryParam(req.query_string, "q") orelse {
        writeJson(stream, 200, "{\"data\":[],\"total\":0}");
        return;
    };

    if (query.len == 0) {
        writeJson(stream, 200, "{\"data\":[],\"total\":0}");
        return;
    }

    // URL decode the query (basic: replace + with space, handle %XX)
    var decoded_buf: [1024]u8 = undefined;
    const decoded = urlDecode(query, &decoded_buf);

    // Use FTS search
    const search_results = db.search("posts", &.{ "title", "body" }, decoded, 25) catch {
        writeJson(stream, 200, "{\"data\":[],\"total\":0}");
        return;
    };

    const tbl = db.table("posts") orelse {
        writeJson(stream, 200, "{\"data\":[],\"total\":0}");
        return;
    };

    var result_rows = std.array_list.Managed(Table.Row).init(allocator);
    defer deinitRowList(&result_rows);

    for (search_results) |sr| {
        const maybe_sr_row = tbl.get(sr.doc_id) catch null;
        if (maybe_sr_row) |sr_row| {
            result_rows.append(sr_row) catch continue;
        }
    }

    // If FTS returned nothing, fall back to simple substring search
    if (result_rows.items.len == 0) {
        var all_rows = collectAllRows(allocator, tbl) catch {
            writeJson(stream, 200, "{\"data\":[],\"total\":0}");
            return;
        };
        defer deinitRowList(&all_rows);

        const lower_query = lowerBuf(decoded);
        for (all_rows.items) |row| {
            const title = getRowStringValue(&row, "title");
            const title_body = getRowStringValue(&row, "body");
            if (containsIgnoreCase(title, lower_query) or containsIgnoreCase(title_body, lower_query)) {
                // Copy the row since all_rows will be deinitialized
                var new_row = Table.Row.init(allocator);
                new_row.id = row.id;
                new_row.created_at = row.created_at;
                new_row.updated_at = row.updated_at;
                new_row.version = row.version;
                // Copy data entries
                var it = row.data.iterator();
                while (it.next()) |entry| {
                    new_row.put(entry.key_ptr.*, entry.value_ptr.*) catch continue;
                }
                result_rows.append(new_row) catch continue;
                if (result_rows.items.len >= 25) break;
            }
        }
    }

    const includes: []const []const u8 = &.{ "author_id", "community_id" };
    const resp = buildViewResponse(allocator, result_rows.items, result_rows.items.len, &post_schema, includes) catch {
        writeJson(stream, 200, "{\"data\":[],\"total\":0}");
        return;
    };
    defer allocator.free(resp);
    writeJson(stream, 200, resp);
}

var lower_buf_storage: [1024]u8 = undefined;

fn lowerBuf(s: []const u8) []const u8 {
    const len = @min(s.len, 1024);
    for (0..len) |i| {
        lower_buf_storage[i] = std.ascii.toLower(s[i]);
    }
    return lower_buf_storage[0..len];
}

fn containsIgnoreCase(haystack: []const u8, needle_lower: []const u8) bool {
    if (needle_lower.len == 0) return true;
    if (haystack.len < needle_lower.len) return false;
    var i: usize = 0;
    while (i + needle_lower.len <= haystack.len) : (i += 1) {
        var found = true;
        for (0..needle_lower.len) |j| {
            if (std.ascii.toLower(haystack[i + j]) != needle_lower[j]) {
                found = false;
                break;
            }
        }
        if (found) return true;
    }
    return false;
}

fn urlDecode(src: []const u8, buf: *[1024]u8) []const u8 {
    var out: usize = 0;
    var i: usize = 0;
    while (i < src.len and out < 1024) {
        if (src[i] == '+') {
            buf[out] = ' ';
            out += 1;
            i += 1;
        } else if (src[i] == '%' and i + 2 < src.len) {
            const high = hexVal(src[i + 1]);
            const low = hexVal(src[i + 2]);
            if (high != null and low != null) {
                buf[out] = (high.? << 4) | low.?;
                out += 1;
                i += 3;
            } else {
                buf[out] = src[i];
                out += 1;
                i += 1;
            }
        } else {
            buf[out] = src[i];
            out += 1;
            i += 1;
        }
    }
    return buf[0..out];
}

fn hexVal(c: u8) ?u8 {
    if (c >= '0' and c <= '9') return c - '0';
    if (c >= 'a' and c <= 'f') return c - 'a' + 10;
    if (c >= 'A' and c <= 'F') return c - 'A' + 10;
    return null;
}

fn handleCreatePost(allocator: Allocator, req: *const HttpRequest, stream: std.net.Stream) void {
    const user_id = authenticateRequest(req) orelse {
        writeErrorJson(stream, 401, "unauthorized");
        return;
    };

    const body = req.body;
    const title = getJsonString(body, "title") orelse {
        writeErrorJson(stream, 400, "title is required");
        return;
    };
    const community_id_str = getJsonString(body, "community_id") orelse {
        writeErrorJson(stream, 400, "community_id is required");
        return;
    };
    const community_id = parseUint64(community_id_str);
    const post_body = getJsonString(body, "body") orelse "";
    const link = getJsonString(body, "link") orelse "";

    if (title.len == 0) {
        writeErrorJson(stream, 400, "title is required");
        return;
    }

    const now_ms = std.time.milliTimestamp();
    const hr = hotRank(1, now_ms);

    var data = std.StringHashMap(Table.Value).init(allocator);
    defer data.deinit();
    data.put("title", Table.Value{ .string = title }) catch return;
    data.put("body", Table.Value{ .string = post_body }) catch return;
    data.put("link", Table.Value{ .string = link }) catch return;
    data.put("image", Table.Value{ .string = "" }) catch return;
    data.put("author_id", Table.Value{ .uint = user_id }) catch return;
    data.put("community_id", Table.Value{ .uint = community_id }) catch return;
    data.put("score", Table.Value{ .int = 1 }) catch return;
    data.put("hot_rank", Table.Value{ .float = hr }) catch return;
    data.put("comment_count", Table.Value{ .int = 0 }) catch return;
    data.put("repost_of", Table.Value{ .uint = 0 }) catch return;

    const post = db.insert("posts", &data) catch {
        writeErrorJson(stream, 500, "failed to create post");
        return;
    };

    const row_json = rowToJson(allocator, &post, &post_schema) catch return;
    defer allocator.free(row_json);

    var buf = std.array_list.Managed(u8).init(allocator);
    defer buf.deinit();
    buf.appendSlice("{\"post\":") catch return;
    buf.appendSlice(row_json) catch return;
    buf.append('}') catch return;

    writeJson(stream, 200, buf.items);
}

// ===== Vote Handler =====

fn handlePostVote(allocator: Allocator, req: *const HttpRequest, stream: std.net.Stream, post_id: u64) void {
    const user_id = authenticateRequest(req) orelse {
        writeErrorJson(stream, 401, "unauthorized");
        return;
    };

    const body = req.body;
    const value_str = getJsonString(body, "value") orelse "0";
    const value = parseInt64(value_str);

    if (value != 1 and value != -1 and value != 0) {
        writeErrorJson(stream, 400, "value must be -1, 0, or 1");
        return;
    }

    const votes_tbl = db.table("votes") orelse {
        writeErrorJson(stream, 500, "votes table not found");
        return;
    };

    // Find existing vote
    var all_votes = collectAllRows(allocator, votes_tbl) catch {
        writeErrorJson(stream, 500, "scan error");
        return;
    };
    defer deinitRowList(&all_votes);

    var existing_vote_id: ?u64 = null;
    for (all_votes.items) |*row| {
        const row_user = getRowUintValue(row, "user_id");
        const row_post = getRowUintValue(row, "post_id");
        if (row_user == user_id and row_post == post_id) {
            existing_vote_id = row.id;
            break;
        }
    }

    if (existing_vote_id) |eid| {
        if (value == 0) {
            _ = db.deleteRow("votes", eid) catch {};
        } else {
            var upd = std.StringHashMap(Table.Value).init(allocator);
            defer upd.deinit();
            upd.put("value", Table.Value{ .int = value }) catch return;
            _ = db.update("votes", eid, &upd) catch {};
        }
    } else if (value != 0) {
        var vote_data = std.StringHashMap(Table.Value).init(allocator);
        defer vote_data.deinit();
        vote_data.put("user_id", Table.Value{ .uint = user_id }) catch return;
        vote_data.put("post_id", Table.Value{ .uint = post_id }) catch return;
        vote_data.put("value", Table.Value{ .int = value }) catch return;
        _ = db.insert("votes", &vote_data) catch {};
    }

    // Recalculate score
    var recalc_votes = collectAllRows(allocator, votes_tbl) catch {
        writeErrorJson(stream, 500, "scan error");
        return;
    };
    defer deinitRowList(&recalc_votes);

    var score: i64 = 0;
    for (recalc_votes.items) |*row| {
        const row_post = getRowUintValue(row, "post_id");
        if (row_post == post_id) {
            score += getRowIntValue(row, "value");
        }
    }

    // Update post score
    const posts_tbl = db.table("posts") orelse return;
    const maybe_vote_post = posts_tbl.get(post_id) catch null;
    if (maybe_vote_post) |vote_post_copy| {
        var post = vote_post_copy;
        defer post.deinit();
        const hr = hotRank(score, post.created_at);
        var upd = std.StringHashMap(Table.Value).init(allocator);
        defer upd.deinit();
        upd.put("score", Table.Value{ .int = score }) catch return;
        upd.put("hot_rank", Table.Value{ .float = hr }) catch return;
        _ = db.update("posts", post_id, &upd) catch {};
    }

    // Response
    var buf: [128]u8 = undefined;
    const resp = std.fmt.bufPrint(&buf, "{{\"score\":{d},\"user_vote\":{d}}}", .{ score, value }) catch return;
    writeJson(stream, 200, resp);
}

// ===== Comment Handlers =====

fn handleGetComments(allocator: Allocator, stream: std.net.Stream, post_id: u64) void {
    const tbl = db.table("comments") orelse {
        writeJson(stream, 200, "{\"data\":[],\"total\":0}");
        return;
    };

    var rows = collectAllRows(allocator, tbl) catch {
        writeJson(stream, 200, "{\"data\":[],\"total\":0}");
        return;
    };
    defer deinitRowList(&rows);

    // Filter by post_id
    var filtered = std.array_list.Managed(Table.Row).init(allocator);
    defer filtered.deinit();

    for (rows.items) |row| {
        const row_post = getRowUintValue(&row, "post_id");
        if (row_post == post_id) {
            filtered.append(row) catch continue;
        }
    }

    // Sort by path asc
    std.mem.sortUnstable(Table.Row, filtered.items, {}, struct {
        fn cmp(_: void, a: Table.Row, b: Table.Row) bool {
            const pa = getRowStringValue(&a, "path");
            const pb = getRowStringValue(&b, "path");
            return std.mem.order(u8, pa, pb) == .lt;
        }
    }.cmp);

    const limit: usize = @min(200, filtered.items.len);
    const includes: []const []const u8 = &.{"author_id"};
    const resp = buildViewResponse(allocator, filtered.items[0..limit], filtered.items.len, &comment_schema, includes) catch {
        writeJson(stream, 200, "{\"data\":[],\"total\":0}");
        return;
    };
    defer allocator.free(resp);
    writeJson(stream, 200, resp);
}

fn handleCreateComment(allocator: Allocator, req: *const HttpRequest, stream: std.net.Stream, post_id: u64) void {
    const user_id = authenticateRequest(req) orelse {
        writeErrorJson(stream, 401, "unauthorized");
        return;
    };

    const body_raw = req.body;
    const comment_body = getJsonString(body_raw, "body") orelse {
        writeErrorJson(stream, 400, "body is required");
        return;
    };
    const parent_id_str = getJsonString(body_raw, "parent_id") orelse "0";
    const parent_id = parseUint64(parent_id_str);

    var depth: i64 = 0;
    var parent_path: []const u8 = "";

    if (parent_id > 0) {
        const comments_tbl = db.table("comments") orelse {
            writeErrorJson(stream, 500, "comments table not found");
            return;
        };
        const maybe_parent = comments_tbl.get(parent_id) catch null;
        if (maybe_parent) |parent_copy| {
            var parent = parent_copy;
            defer parent.deinit();
            depth = getRowIntValue(&parent, "depth") + 1;
            parent_path = getRowStringValue(&parent, "path");
            // We need to copy the path since parent will be deinitialized
            // Actually we'll build the path below after insert
        }
    }

    var data = std.StringHashMap(Table.Value).init(allocator);
    defer data.deinit();
    data.put("body", Table.Value{ .string = comment_body }) catch return;
    data.put("author_id", Table.Value{ .uint = user_id }) catch return;
    data.put("post_id", Table.Value{ .uint = post_id }) catch return;
    data.put("parent_id", Table.Value{ .uint = parent_id }) catch return;
    data.put("depth", Table.Value{ .int = depth }) catch return;
    data.put("path", Table.Value{ .string = "" }) catch return;
    data.put("score", Table.Value{ .int = 0 }) catch return;

    const comment = db.insert("comments", &data) catch {
        writeErrorJson(stream, 500, "failed to create comment");
        return;
    };

    // Update comment path
    // Re-read parent path if needed
    var actual_parent_path: []const u8 = "";
    var path_owned: ?[]u8 = null;
    if (parent_id > 0) {
        const comments_tbl = db.table("comments") orelse {
            writeErrorJson(stream, 500, "comments table not found");
            return;
        };
        const maybe_parent2 = comments_tbl.get(parent_id) catch null;
        if (maybe_parent2) |pp_copy| {
            var prnt = pp_copy;
            defer prnt.deinit();
            const pp_str = getRowStringValue(&prnt, "path");
            path_owned = allocator.dupe(u8, pp_str) catch null;
            if (path_owned) |po| {
                actual_parent_path = po;
            }
        }
    }
    defer if (path_owned) |po| allocator.free(po);

    var path_buf: [256]u8 = undefined;
    const comment_path = std.fmt.bufPrint(&path_buf, "{s}/{d:0>8}", .{ actual_parent_path, comment.id }) catch "";

    var path_upd = std.StringHashMap(Table.Value).init(allocator);
    defer path_upd.deinit();
    path_upd.put("path", Table.Value{ .string = comment_path }) catch return;
    _ = db.update("comments", comment.id, &path_upd) catch {};

    // Update post comment_count
    const comments_tbl2 = db.table("comments") orelse return;
    var all_comments = collectAllRows(allocator, comments_tbl2) catch return;
    defer deinitRowList(&all_comments);

    var count: i64 = 0;
    for (all_comments.items) |*row| {
        if (getRowUintValue(row, "post_id") == post_id) {
            count += 1;
        }
    }

    var post_upd = std.StringHashMap(Table.Value).init(allocator);
    defer post_upd.deinit();
    post_upd.put("comment_count", Table.Value{ .int = count }) catch return;
    _ = db.update("posts", post_id, &post_upd) catch {};

    const row_json = rowToJson(allocator, &comment, &comment_schema) catch return;
    defer allocator.free(row_json);

    var buf = std.array_list.Managed(u8).init(allocator);
    defer buf.deinit();
    buf.appendSlice("{\"comment\":") catch return;
    buf.appendSlice(row_json) catch return;
    buf.append('}') catch return;

    writeJson(stream, 200, buf.items);
}

// ===== Comment Vote Handler =====

fn handleCommentVote(allocator: Allocator, req: *const HttpRequest, stream: std.net.Stream, comment_id: u64) void {
    const user_id = authenticateRequest(req) orelse {
        writeErrorJson(stream, 401, "unauthorized");
        return;
    };

    const body = req.body;
    const value_str = getJsonString(body, "value") orelse "0";
    const value = parseInt64(value_str);

    if (value != 1 and value != -1 and value != 0) {
        writeErrorJson(stream, 400, "value must be -1, 0, or 1");
        return;
    }

    const cv_tbl = db.table("comment_votes") orelse {
        writeErrorJson(stream, 500, "comment_votes table not found");
        return;
    };

    // Find existing vote
    var all_cv = collectAllRows(allocator, cv_tbl) catch {
        writeErrorJson(stream, 500, "scan error");
        return;
    };
    defer deinitRowList(&all_cv);

    var existing_id: ?u64 = null;
    for (all_cv.items) |*row| {
        const row_user = getRowUintValue(row, "user_id");
        const row_comment = getRowUintValue(row, "comment_id");
        if (row_user == user_id and row_comment == comment_id) {
            existing_id = row.id;
            break;
        }
    }

    if (existing_id) |eid| {
        if (value == 0) {
            _ = db.deleteRow("comment_votes", eid) catch {};
        } else {
            var upd = std.StringHashMap(Table.Value).init(allocator);
            defer upd.deinit();
            upd.put("value", Table.Value{ .int = value }) catch return;
            _ = db.update("comment_votes", eid, &upd) catch {};
        }
    } else if (value != 0) {
        var vote_data = std.StringHashMap(Table.Value).init(allocator);
        defer vote_data.deinit();
        vote_data.put("user_id", Table.Value{ .uint = user_id }) catch return;
        vote_data.put("comment_id", Table.Value{ .uint = comment_id }) catch return;
        vote_data.put("value", Table.Value{ .int = value }) catch return;
        _ = db.insert("comment_votes", &vote_data) catch {};
    }

    // Recalculate score
    var recalc_cv = collectAllRows(allocator, cv_tbl) catch {
        writeErrorJson(stream, 500, "scan error");
        return;
    };
    defer deinitRowList(&recalc_cv);

    var score: i64 = 0;
    for (recalc_cv.items) |*row| {
        const row_comment = getRowUintValue(row, "comment_id");
        if (row_comment == comment_id) {
            score += getRowIntValue(row, "value");
        }
    }

    // Update comment score
    var upd = std.StringHashMap(Table.Value).init(allocator);
    defer upd.deinit();
    upd.put("score", Table.Value{ .int = score }) catch return;
    _ = db.update("comments", comment_id, &upd) catch {};

    var buf: [128]u8 = undefined;
    const resp = std.fmt.bufPrint(&buf, "{{\"score\":{d},\"user_vote\":{d}}}", .{ score, value }) catch return;
    writeJson(stream, 200, resp);
}

// ===== Static File Serving =====

fn handleStaticFile(stream: std.net.Stream, file_path: []const u8) void {
    // Prevent path traversal
    if (std.mem.indexOf(u8, file_path, "..") != null) {
        writeResponse(stream, 403, "Forbidden", "text/plain", "forbidden");
        return;
    }

    var path_buf: [512]u8 = undefined;
    const full_path = std.fmt.bufPrint(&path_buf, "web/assets/{s}", .{file_path}) catch {
        writeResponse(stream, 404, "Not Found", "text/plain", "not found");
        return;
    };

    const file = std.fs.cwd().openFile(full_path, .{}) catch {
        writeResponse(stream, 404, "Not Found", "text/plain", "not found");
        return;
    };
    defer file.close();

    const stat = file.stat() catch {
        writeResponse(stream, 500, "Internal Server Error", "text/plain", "error");
        return;
    };

    const content = file.readToEndAlloc(global_allocator, 10 * 1024 * 1024) catch {
        writeResponse(stream, 500, "Internal Server Error", "text/plain", "error");
        return;
    };
    defer global_allocator.free(content);

    // Determine content type
    const ct = guessContentType(file_path);

    var header_buf: [1024]u8 = undefined;
    const header = std.fmt.bufPrint(&header_buf, "HTTP/1.1 200 OK\r\nContent-Type: {s}\r\nContent-Length: {d}\r\nCache-Control: no-store, max-age=0\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n", .{ ct, stat.size }) catch return;
    _ = stream.write(header) catch return;
    _ = stream.write(content) catch return;
}

fn guessContentType(path: []const u8) []const u8 {
    if (std.mem.endsWith(u8, path, ".js")) return "application/javascript";
    if (std.mem.endsWith(u8, path, ".css")) return "text/css";
    if (std.mem.endsWith(u8, path, ".html")) return "text/html";
    if (std.mem.endsWith(u8, path, ".svg")) return "image/svg+xml";
    if (std.mem.endsWith(u8, path, ".png")) return "image/png";
    if (std.mem.endsWith(u8, path, ".jpg") or std.mem.endsWith(u8, path, ".jpeg")) return "image/jpeg";
    if (std.mem.endsWith(u8, path, ".ico")) return "image/x-icon";
    if (std.mem.endsWith(u8, path, ".json")) return "application/json";
    if (std.mem.endsWith(u8, path, ".map")) return "application/json";
    if (std.mem.endsWith(u8, path, ".woff2")) return "font/woff2";
    if (std.mem.endsWith(u8, path, ".woff")) return "font/woff";
    return "application/octet-stream";
}

// ===== SPA HTML Shell =====

fn serveSpaHtml(stream: std.net.Stream) void {
    const html =
        \\<!doctype html>
        \\<html lang="en">
        \\  <head>
        \\    <meta charset="utf-8" />
        \\    <meta name="viewport" content="width=device-width, initial-scale=1" />
        \\    <title>Leddit - A Reddit Clone</title>
        \\    <link rel="icon" href="/static/favicon.svg" type="image/svg+xml" />
        \\    <link rel="preconnect" href="https://fonts.googleapis.com" />
        \\    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
        \\    <link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600;700&display=swap" rel="stylesheet" />
        \\    <link rel="stylesheet" href="/static/app.css" />
        \\  </head>
        \\  <body>
        \\    <div id="app"></div>
        \\    <script>
        \\      window.__FLOP_INITIAL_PATH__ = "/";
        \\    </script>
        \\    <script type="module" src="/static/app.js"></script>
        \\  </body>
        \\</html>
    ;

    writeResponse(stream, 200, "OK", "text/html; charset=utf-8", html);
}

// ===== TCP Server =====

fn handleConnection(conn: std.net.Server.Connection) void {
    defer conn.stream.close();

    // Read the request
    var buf: [65536]u8 = undefined;
    var total_read: usize = 0;

    // Read available data
    const n = conn.stream.read(buf[total_read..]) catch return;
    if (n == 0) return;
    total_read += n;

    // If we have headers, check if we need more body data
    if (findSubstring(buf[0..total_read], "\r\n\r\n")) |header_end| {
        // Check Content-Length
        const headers = buf[0..header_end];
        var content_length: usize = 0;
        var line_start: usize = 0;
        while (line_start < headers.len) {
            const remaining = headers[line_start..];
            const line_end = findSubstring(remaining, "\r\n") orelse remaining.len;
            const line = remaining[0..line_end];
            if (asciiEqlIgnoreCase(line[0..@min(line.len, 16)], "content-length: "[0..@min(line.len, 16)])) {
                // Find the colon
                if (std.mem.indexOfScalar(u8, line, ':')) |colon| {
                    const val = std.mem.trim(u8, line[colon + 1 ..], " ");
                    content_length = std.fmt.parseInt(usize, val, 10) catch 0;
                }
            }
            line_start += line_end + 2;
        }

        const body_start = header_end + 4;
        const body_needed = content_length;
        const body_have = if (total_read > body_start) total_read - body_start else 0;

        // Read remaining body if needed
        if (body_have < body_needed) {
            var remaining_to_read = body_needed - body_have;
            while (remaining_to_read > 0 and total_read < buf.len) {
                const m = conn.stream.read(buf[total_read..]) catch break;
                if (m == 0) break;
                total_read += m;
                if (m >= remaining_to_read) break;
                remaining_to_read -= m;
            }
        }
    }

    const request_data = buf[0..total_read];

    const req = parseHttpRequest(request_data) orelse return;

    // Use an arena allocator for per-request allocations
    var arena = std.heap.ArenaAllocator.init(global_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    handleRequest(allocator, &req, conn.stream);
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    global_allocator = gpa.allocator();

    // Initialize database
    db = try Database.open(global_allocator, "data");

    // Create all tables
    _ = try db.createTable(&user_schema);
    _ = try db.createTable(&community_schema);
    _ = try db.createTable(&membership_schema);
    _ = try db.createTable(&post_schema);
    _ = try db.createTable(&comment_schema);
    _ = try db.createTable(&vote_schema);
    _ = try db.createTable(&comment_vote_schema);

    // Initialize auth manager
    auth_mgr = Auth.AuthManager.init(global_allocator);

    // Determine port
    var port: u16 = 3000;
    if (std.process.getEnvVarOwned(global_allocator, "PORT")) |port_str| {
        defer global_allocator.free(port_str);
        port = std.fmt.parseInt(u16, port_str, 10) catch 3000;
    } else |_| {}

    // Start TCP server
    const addr = std.net.Address.parseIp4("0.0.0.0", port) catch unreachable;
    var server = try addr.listen(.{
        .reuse_address = true,
    });
    defer server.deinit();

    std.log.info("Reddit clone running on http://localhost:{d}", .{port});

    // Accept connections
    while (true) {
        const conn = server.accept() catch |err| {
            std.log.err("accept error: {}", .{err});
            continue;
        };

        // Handle synchronously (simple approach)
        handleConnection(conn);
    }
}