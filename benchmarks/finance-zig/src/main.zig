const std = @import("std");
const flop = @import("flop");
const Database = flop.Database.Database;
const table_mod = flop.Table;
const Value = table_mod.Value;
const Row = table_mod.Row;
const Auth = flop.Auth.AuthManager;

// ─── Schemas ────────────────────────────────────────────────────────────────

const user_schema = table_mod.Schema{
    .name = "users",
    .fields = &[_]table_mod.Field{
        .{ .name = "email", .field_type = .string, .required = true, .unique = true, .max_len = 255 },
        .{ .name = "password", .field_type = .string, .required = true },
        .{ .name = "name", .field_type = .string, .max_len = 100 },
    },
};

const account_schema = table_mod.Schema{
    .name = "accounts",
    .fields = &[_]table_mod.Field{
        .{ .name = "owner_id", .field_type = .ref, .ref_table = "users", .required = true, .indexed = true },
        .{ .name = "name", .field_type = .string, .required = true, .max_len = 200 },
        .{ .name = "type", .field_type = .string, .required = true },
        .{ .name = "currency", .field_type = .string, .required = true },
        .{ .name = "balance", .field_type = .float },
    },
};

const transaction_schema = table_mod.Schema{
    .name = "transactions",
    .fields = &[_]table_mod.Field{
        .{ .name = "from_account", .field_type = .ref, .ref_table = "accounts", .indexed = true },
        .{ .name = "to_account", .field_type = .ref, .ref_table = "accounts", .required = true, .indexed = true },
        .{ .name = "amount", .field_type = .float, .required = true },
        .{ .name = "type", .field_type = .string, .required = true },
        .{ .name = "description", .field_type = .string, .max_len = 500 },
        .{ .name = "status", .field_type = .string, .required = true },
    },
};

// ─── Globals ────────────────────────────────────────────────────────────────

var db: Database = undefined;
var auth: Auth = undefined;
var total_balance_cents: std.atomic.Value(i64) = std.atomic.Value(i64).init(0);

// ─── JSON helpers ───────────────────────────────────────────────────────────

const JsonWriter = struct {
    buf: std.array_list.Managed(u8),

    fn init(allocator: std.mem.Allocator) JsonWriter {
        return .{ .buf = std.array_list.Managed(u8).init(allocator) };
    }

    fn deinit(self: *JsonWriter) void {
        self.buf.deinit();
    }

    fn toOwnedSlice(self: *JsonWriter) ![]u8 {
        return self.buf.toOwnedSlice();
    }

    fn objectStart(self: *JsonWriter) !void {
        try self.buf.append('{');
    }

    fn objectEnd(self: *JsonWriter) !void {
        if (self.buf.items.len > 0 and self.buf.items[self.buf.items.len - 1] == ',') {
            self.buf.items.len -= 1;
        }
        try self.buf.append('}');
    }

    fn arrayStart(self: *JsonWriter) !void {
        try self.buf.append('[');
    }

    fn arrayEnd(self: *JsonWriter) !void {
        if (self.buf.items.len > 0 and self.buf.items[self.buf.items.len - 1] == ',') {
            self.buf.items.len -= 1;
        }
        try self.buf.append(']');
    }

    fn writeKey(self: *JsonWriter, k: []const u8) !void {
        try self.buf.append('"');
        try self.buf.appendSlice(k);
        try self.buf.appendSlice("\":");
    }

    fn writeString(self: *JsonWriter, v: []const u8) !void {
        try self.buf.append('"');
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
                        const hex = "0123456789abcdef";
                        try self.buf.append(hex[c >> 4]);
                        try self.buf.append(hex[c & 0xf]);
                    } else {
                        try self.buf.append(c);
                    }
                },
            }
        }
        try self.buf.append('"');
        try self.buf.append(',');
    }

    fn writeInt(self: *JsonWriter, v: i64) !void {
        var num_buf: [32]u8 = undefined;
        const s = std.fmt.bufPrint(&num_buf, "{d}", .{v}) catch unreachable;
        try self.buf.appendSlice(s);
        try self.buf.append(',');
    }

    fn writeUint(self: *JsonWriter, v: u64) !void {
        var num_buf: [32]u8 = undefined;
        const s = std.fmt.bufPrint(&num_buf, "{d}", .{v}) catch unreachable;
        try self.buf.appendSlice(s);
        try self.buf.append(',');
    }

    fn writeFloat(self: *JsonWriter, v: f64) !void {
        // Format float: if it's a whole number, print without decimals
        const rounded = @round(v);
        if (v == rounded and @abs(v) < 1e15) {
            const iv: i64 = @intFromFloat(rounded);
            var num_buf: [32]u8 = undefined;
            const s = std.fmt.bufPrint(&num_buf, "{d}", .{iv}) catch unreachable;
            try self.buf.appendSlice(s);
        } else {
            var num_buf: [64]u8 = undefined;
            const s = std.fmt.bufPrint(&num_buf, "{d}", .{v}) catch unreachable;
            try self.buf.appendSlice(s);
        }
        try self.buf.append(',');
    }

    fn writeNull(self: *JsonWriter) !void {
        try self.buf.appendSlice("null,");
    }

    fn comma(self: *JsonWriter) !void {
        try self.buf.append(',');
    }
};

// ─── Simple JSON body parser ────────────────────────────────────────────────

const JsonValue = union(enum) {
    string: []const u8,
    number: f64,
    boolean: bool,
    null_val: void,
};

fn parseJsonBody(body: []const u8) std.StringHashMap(JsonValue) {
    var map = std.StringHashMap(JsonValue).init(std.heap.page_allocator);

    var i: usize = 0;
    // Skip to first {
    while (i < body.len and body[i] != '{') : (i += 1) {}
    if (i >= body.len) return map;
    i += 1;

    while (i < body.len) {
        // Skip whitespace and commas
        while (i < body.len and (body[i] == ' ' or body[i] == '\n' or body[i] == '\r' or body[i] == '\t' or body[i] == ',')) : (i += 1) {}
        if (i >= body.len or body[i] == '}') break;

        // Parse key
        if (body[i] != '"') break;
        i += 1;
        const key_start = i;
        while (i < body.len and body[i] != '"') : (i += 1) {}
        const json_key = body[key_start..i];
        if (i >= body.len) break;
        i += 1; // skip closing "

        // Skip : and whitespace
        while (i < body.len and (body[i] == ':' or body[i] == ' ')) : (i += 1) {}

        if (i >= body.len) break;
        if (body[i] == '"') {
            // String value
            i += 1;
            const val_start = i;
            while (i < body.len and body[i] != '"') {
                if (body[i] == '\\') {
                    i += 1;
                }
                i += 1;
            }
            const val = body[val_start..i];
            if (i < body.len) i += 1; // skip closing "
            map.put(json_key, .{ .string = val }) catch {};
        } else if (body[i] == 'n') {
            i += 4;
            map.put(json_key, .{ .null_val = {} }) catch {};
        } else if (body[i] == 't') {
            i += 4;
            map.put(json_key, .{ .boolean = true }) catch {};
        } else if (body[i] == 'f') {
            i += 5;
            map.put(json_key, .{ .boolean = false }) catch {};
        } else {
            // Number
            const val_start = i;
            while (i < body.len and body[i] != ',' and body[i] != '}' and body[i] != ' ' and body[i] != '\n') : (i += 1) {}
            const num_str = body[val_start..i];
            const num = std.fmt.parseFloat(f64, num_str) catch 0.0;
            map.put(json_key, .{ .number = num }) catch {};
        }
    }

    return map;
}

fn jsonStr(m: *std.StringHashMap(JsonValue), key: []const u8) []const u8 {
    if (m.get(key)) |v| {
        switch (v) {
            .string => |s| return s,
            else => return "",
        }
    }
    return "";
}

fn jsonF64(m: *std.StringHashMap(JsonValue), key: []const u8) f64 {
    if (m.get(key)) |v| {
        switch (v) {
            .number => |n| return n,
            .string => |s| return std.fmt.parseFloat(f64, s) catch 0.0,
            else => return 0.0,
        }
    }
    return 0.0;
}

fn jsonID(m: *std.StringHashMap(JsonValue), key: []const u8) u64 {
    if (m.get(key)) |v| {
        switch (v) {
            .number => |n| return if (n >= 0) @intFromFloat(n) else 0,
            .string => |s| return std.fmt.parseInt(u64, s, 10) catch 0,
            else => return 0,
        }
    }
    return 0;
}

// ─── URL query parsing ──────────────────────────────────────────────────────

fn getQueryParam(query: []const u8, param_name: []const u8) ?[]const u8 {
    var rest = query;
    while (rest.len > 0) {
        // Find key=value pair
        const end = std.mem.indexOfScalar(u8, rest, '&') orelse rest.len;
        const pair = rest[0..end];
        if (std.mem.indexOfScalar(u8, pair, '=')) |eq_pos| {
            const k = pair[0..eq_pos];
            const v = pair[eq_pos + 1 ..];
            if (std.mem.eql(u8, k, param_name)) {
                return v;
            }
        }
        if (end >= rest.len) break;
        rest = rest[end + 1 ..];
    }
    return null;
}

// ─── SHA256 helper ──────────────────────────────────────────────────────────

fn sha256Hex(input: []const u8) [64]u8 {
    var hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(input, &hash, .{});
    var hex: [64]u8 = undefined;
    const charset = "0123456789abcdef";
    for (hash, 0..) |byte, i| {
        hex[i * 2] = charset[byte >> 4];
        hex[i * 2 + 1] = charset[byte & 0x0f];
    }
    return hex;
}

// ─── Row value helpers ──────────────────────────────────────────────────────

fn rowGetStr(row: *const Row, key: []const u8) []const u8 {
    if (row.data.get(key)) |v| {
        switch (v) {
            .string => |s| return s,
            else => return "",
        }
    }
    return "";
}

fn rowGetF64(row: *const Row, key: []const u8) f64 {
    if (row.data.get(key)) |v| {
        switch (v) {
            .float => |f| return f,
            .string => |s| {
                // The DB stores encoded floats as strings. Try to parse.
                if (s.len == 8) {
                    const decoded = flop.Encoding.decodeF64(s[0..8]);
                    return decoded;
                }
                return std.fmt.parseFloat(f64, s) catch 0.0;
            },
            .int => |i| return @floatFromInt(i),
            .uint => |u| return @floatFromInt(u),
            else => return 0.0,
        }
    }
    return 0.0;
}

fn rowGetU64(row: *const Row, key: []const u8) u64 {
    if (row.data.get(key)) |v| {
        switch (v) {
            .uint => |u| return u,
            .int => |i| return if (i >= 0) @intCast(i) else 0,
            .string => |s| {
                if (s.len == 8) {
                    return flop.Encoding.decodeU64(s[0..8]);
                }
                return std.fmt.parseInt(u64, s, 10) catch 0;
            },
            .float => |f| return if (f >= 0) @intFromFloat(f) else 0,
            else => return 0,
        }
    }
    return 0;
}

// ─── Auth from request ──────────────────────────────────────────────────────

fn authFromHeader(header: []const u8) u64 {
    const prefix = "Bearer ";
    if (header.len > prefix.len and std.mem.startsWith(u8, header, prefix)) {
        const token = header[prefix.len..];
        if (auth.validateToken(token)) |uid| {
            return uid;
        }
    }
    return 0;
}

// ─── HTTP server using raw TCP ──────────────────────────────────────────────

const HttpRequest = struct {
    method: []const u8,
    path: []const u8,
    query: []const u8,
    body: []const u8,
    auth_header: []const u8,
};

fn parseHttpRequest(raw: []const u8) ?HttpRequest {
    // Find end of request line
    const line_end = std.mem.indexOf(u8, raw, "\r\n") orelse return null;
    const request_line = raw[0..line_end];

    // Parse method and path
    var parts_iter = std.mem.splitScalar(u8, request_line, ' ');
    const method = parts_iter.next() orelse return null;
    const full_path = parts_iter.next() orelse return null;

    // Split path and query
    var path: []const u8 = full_path;
    var query: []const u8 = "";
    if (std.mem.indexOfScalar(u8, full_path, '?')) |qpos| {
        path = full_path[0..qpos];
        query = full_path[qpos + 1 ..];
    }

    // Find authorization header
    var auth_header: []const u8 = "";
    var header_start = line_end + 2;
    while (header_start < raw.len) {
        const next_end = std.mem.indexOf(u8, raw[header_start..], "\r\n") orelse break;
        const header_line = raw[header_start .. header_start + next_end];
        if (header_line.len == 0) break;
        // Case-insensitive check for Authorization
        if (header_line.len > 15) {
            var lower_buf: [32]u8 = undefined;
            const check_len = @min(header_line.len, 14);
            for (0..check_len) |ci| {
                lower_buf[ci] = std.ascii.toLower(header_line[ci]);
            }
            if (std.mem.startsWith(u8, lower_buf[0..check_len], "authorization:")) {
                var val = header_line[14..];
                while (val.len > 0 and val[0] == ' ') val = val[1..];
                auth_header = val;
            }
        }
        header_start += next_end + 2;
    }

    // Find body (after \r\n\r\n)
    var body: []const u8 = "";
    if (std.mem.indexOf(u8, raw, "\r\n\r\n")) |body_start| {
        body = raw[body_start + 4 ..];
    }

    return .{
        .method = method,
        .path = path,
        .query = query,
        .body = body,
        .auth_header = auth_header,
    };
}

fn sendResponse(conn: std.net.Stream, status: u16, body_content: []const u8) void {
    const status_text: []const u8 = switch (status) {
        200 => "200 OK",
        400 => "400 Bad Request",
        401 => "401 Unauthorized",
        404 => "404 Not Found",
        409 => "409 Conflict",
        500 => "500 Internal Server Error",
        else => "200 OK",
    };

    var header_buf: [512]u8 = undefined;
    const header = std.fmt.bufPrint(&header_buf, "HTTP/1.1 {s}\r\nContent-Type: application/json\r\nContent-Length: {d}\r\nConnection: keep-alive\r\n\r\n", .{ status_text, body_content.len }) catch return;

    // Write header and body
    conn.writeAll(header) catch return;
    if (body_content.len > 0) {
        conn.writeAll(body_content) catch return;
    }
}

fn sendJson(conn: std.net.Stream, status: u16, json_body: []const u8) void {
    sendResponse(conn, status, json_body);
}

// ─── Handler dispatch ───────────────────────────────────────────────────────

fn handleConnection(conn: std.net.Stream) void {
    defer conn.close();

    var buf: [65536]u8 = undefined;
    var total_read: usize = 0;

    // Read the full request, handling partial reads
    while (total_read < buf.len) {
        const n = conn.read(buf[total_read..]) catch return;
        if (n == 0) return; // Connection closed
        total_read += n;

        // Check if we have a complete request
        const data = buf[0..total_read];
        if (std.mem.indexOf(u8, data, "\r\n\r\n")) |header_end| {
            // Check Content-Length
            var content_length: usize = 0;
            var h_start: usize = 0;
            while (h_start < header_end) {
                const h_end = std.mem.indexOf(u8, data[h_start..header_end], "\r\n") orelse break;
                const h_line = data[h_start .. h_start + h_end];
                if (h_line.len > 16) {
                    var lower_check: [20]u8 = undefined;
                    const clen = @min(h_line.len, 16);
                    for (0..clen) |ci| {
                        lower_check[ci] = std.ascii.toLower(h_line[ci]);
                    }
                    if (std.mem.startsWith(u8, lower_check[0..clen], "content-length:")) {
                        var val = h_line[15..];
                        while (val.len > 0 and val[0] == ' ') val = val[1..];
                        content_length = std.fmt.parseInt(usize, val, 10) catch 0;
                    }
                }
                h_start += h_end + 2;
            }

            const body_start = header_end + 4;
            const body_received = total_read - body_start;
            if (body_received >= content_length) {
                break; // We have the complete request
            }
        }
    }

    const raw = buf[0..total_read];
    const req = parseHttpRequest(raw) orelse return;

    // Route the request
    if (std.mem.eql(u8, req.method, "POST")) {
        if (std.mem.eql(u8, req.path, "/api/auth/register")) {
            handleRegister(conn, &req);
        } else if (std.mem.eql(u8, req.path, "/api/auth/password")) {
            handleLogin(conn, &req);
        } else if (std.mem.startsWith(u8, req.path, "/api/reduce/")) {
            handleReduce(conn, &req);
        } else {
            sendJson(conn, 404, "{\"error\":\"not found\"}");
        }
    } else if (std.mem.eql(u8, req.method, "GET")) {
        if (std.mem.startsWith(u8, req.path, "/api/view/")) {
            handleView(conn, &req);
        } else {
            sendJson(conn, 404, "{\"error\":\"not found\"}");
        }
    } else {
        sendJson(conn, 404, "{\"error\":\"not found\"}");
    }
}

// ─── Auth handlers ──────────────────────────────────────────────────────────

fn handleRegister(conn: std.net.Stream, req: *const HttpRequest) void {
    var body_map = parseJsonBody(req.body);
    defer body_map.deinit();

    const email = jsonStr(&body_map, "email");
    const password = jsonStr(&body_map, "password");
    const name = jsonStr(&body_map, "name");

    if (email.len == 0 or password.len == 0) {
        sendJson(conn, 400, "{\"error\":\"email and password required\"}");
        return;
    }

    const hash = sha256Hex(password);

    var data = std.StringHashMap(Value).init(std.heap.page_allocator);
    defer data.deinit();
    data.put("email", .{ .string = email }) catch {};
    data.put("password", .{ .string = &hash }) catch {};
    data.put("name", .{ .string = name }) catch {};

    const row = db.insert("users", &data) catch {
        sendJson(conn, 500, "{\"error\":\"insert failed\"}");
        return;
    };
    defer {
        var owned = row;
        owned.deinit();
    }

    const token = auth.makeToken(row.id) catch {
        sendJson(conn, 500, "{\"error\":\"token generation failed\"}");
        return;
    };

    // Build response
    var jw = JsonWriter.init(std.heap.page_allocator);
    defer jw.deinit();
    jw.objectStart() catch return;
    jw.writeKey("token") catch return;
    jw.writeString(token) catch return;
    jw.objectEnd() catch return;

    const resp = jw.toOwnedSlice() catch return;
    defer std.heap.page_allocator.free(resp);
    sendJson(conn, 200, resp);
}

fn handleLogin(conn: std.net.Stream, req: *const HttpRequest) void {
    var body_map = parseJsonBody(req.body);
    defer body_map.deinit();

    const email = jsonStr(&body_map, "email");
    const password = jsonStr(&body_map, "password");
    const hash = sha256Hex(password);
    const hash_slice: []const u8 = &hash;

    // Scan users to find match
    const users_tbl = db.table("users") orelse {
        sendJson(conn, 500, "{\"error\":\"users table not found\"}");
        return;
    };

    var found_id: u64 = 0;

    // Scan pages directly
    users_tbl.mutex.lock();
    defer users_tbl.mutex.unlock();

    var page_id = users_tbl.pager.root_page_id;
    while (page_id != 0) {
        const page = users_tbl.pager.readPage(page_id) catch break;
        const n = page.numEntries();
        var i: u16 = 0;
        while (i < n) : (i += 1) {
            if (page.entryAt(i)) |entry| {
                if (entry.value.len >= 32) {
                    // Decode minimal: check email and password fields
                    var row = decodeRowBasic(entry.value) orelse continue;
                    defer row.deinit();
                    const row_email = rowGetStr(&row, "email");
                    const row_pass = rowGetStr(&row, "password");
                    if (std.mem.eql(u8, row_email, email) and std.mem.eql(u8, row_pass, hash_slice)) {
                        found_id = row.id;
                        break;
                    }
                }
            }
        }
        if (found_id != 0) break;
        page_id = page.overflowId();
    }

    if (found_id == 0) {
        sendJson(conn, 401, "{\"error\":\"invalid credentials\"}");
        return;
    }

    const token = auth.makeToken(found_id) catch {
        sendJson(conn, 500, "{\"error\":\"token generation failed\"}");
        return;
    };

    var jw = JsonWriter.init(std.heap.page_allocator);
    defer jw.deinit();
    jw.objectStart() catch return;
    jw.writeKey("token") catch return;
    jw.writeString(token) catch return;
    jw.objectEnd() catch return;

    const resp = jw.toOwnedSlice() catch return;
    defer std.heap.page_allocator.free(resp);
    sendJson(conn, 200, resp);
}

// ─── Decode row from raw page data ─────────────────────────────────────────

fn decodeRowBasic(raw: []const u8) ?Row {
    const encoding = flop.Encoding;
    if (raw.len < encoding.ROW_HEADER_SIZE) return null;

    var row = Row.init(std.heap.page_allocator);
    row.id = encoding.decodeU64(raw[0..8]);
    row.created_at = encoding.decodeI64(raw[8..16]);
    row.updated_at = encoding.decodeI64(raw[16..24]);
    row.version = encoding.decodeU64(raw[24..32]);

    var offset: usize = encoding.ROW_HEADER_SIZE;
    while (offset + 6 <= raw.len) {
        const name_len = std.mem.readInt(u16, raw[offset..][0..2], .little);
        offset += 2;
        if (offset + name_len + 4 > raw.len) break;
        const name_val = raw[offset .. offset + name_len];
        offset += name_len;
        const data_len = std.mem.readInt(u32, raw[offset..][0..4], .little);
        offset += 4;
        if (offset + data_len > raw.len) break;
        const field_data = raw[offset .. offset + data_len];
        offset += data_len;

        row.put(name_val, Value{ .string = field_data }) catch {};
    }
    return row;
}

// ─── Reduce handlers ────────────────────────────────────────────────────────

fn handleReduce(conn: std.net.Stream, req: *const HttpRequest) void {
    const uid = authFromHeader(req.auth_header);
    if (uid == 0) {
        sendJson(conn, 401, "{\"error\":\"unauthorized\"}");
        return;
    }

    // Extract reducer name from /api/reduce/{name}
    const prefix = "/api/reduce/";
    if (req.path.len <= prefix.len) {
        sendJson(conn, 404, "{\"error\":\"unknown reducer\"}");
        return;
    }
    const name = req.path[prefix.len..];

    if (std.mem.eql(u8, name, "create_account")) {
        handleCreateAccount(conn, uid, req);
    } else if (std.mem.eql(u8, name, "deposit")) {
        handleDeposit(conn, uid, req);
    } else if (std.mem.eql(u8, name, "transfer") or std.mem.eql(u8, name, "edit_transfer")) {
        handleTransfer(conn, uid, req);
    } else {
        var err_buf: [128]u8 = undefined;
        const err_msg = std.fmt.bufPrint(&err_buf, "{{\"error\":\"unknown reducer: {s}\"}}", .{name}) catch return;
        sendJson(conn, 404, err_msg);
    }
}

fn handleCreateAccount(conn: std.net.Stream, uid: u64, req: *const HttpRequest) void {
    var body_map = parseJsonBody(req.body);
    defer body_map.deinit();

    var data = std.StringHashMap(Value).init(std.heap.page_allocator);
    defer data.deinit();
    data.put("owner_id", .{ .uint = uid }) catch {};
    data.put("name", .{ .string = jsonStr(&body_map, "name") }) catch {};
    data.put("type", .{ .string = jsonStr(&body_map, "type") }) catch {};
    data.put("currency", .{ .string = jsonStr(&body_map, "currency") }) catch {};
    data.put("balance", .{ .float = 0.0 }) catch {};

    const row = db.insert("accounts", &data) catch {
        sendJson(conn, 500, "{\"error\":\"insert failed\"}");
        return;
    };
    defer {
        var owned = row;
        owned.deinit();
    }

    var id_buf: [32]u8 = undefined;
    const id_str = std.fmt.bufPrint(&id_buf, "{d}", .{row.id}) catch return;

    var jw = JsonWriter.init(std.heap.page_allocator);
    defer jw.deinit();
    jw.objectStart() catch return;
    jw.writeKey("data") catch return;
    jw.objectStart() catch return;
    jw.writeKey("id") catch return;
    jw.writeString(id_str) catch return;
    jw.objectEnd() catch return;
    jw.objectEnd() catch return;

    const resp = jw.toOwnedSlice() catch return;
    defer std.heap.page_allocator.free(resp);
    sendJson(conn, 200, resp);
}

fn handleDeposit(conn: std.net.Stream, uid: u64, req: *const HttpRequest) void {
    _ = uid;
    var body_map = parseJsonBody(req.body);
    defer body_map.deinit();

    const acc_id = jsonID(&body_map, "accountId");
    const amount = jsonF64(&body_map, "amount");

    if (acc_id == 0 or amount <= 0) {
        sendJson(conn, 400, "{\"error\":\"invalid accountId or amount\"}");
        return;
    }

    const accounts_tbl = db.table("accounts") orelse {
        sendJson(conn, 404, "{\"error\":\"account not found\"}");
        return;
    };

    // Get current account
    const acc_row_opt = accounts_tbl.get(acc_id) catch null;
    if (acc_row_opt == null) {
        sendJson(conn, 404, "{\"error\":\"account not found\"}");
        return;
    }
    var acc_row = acc_row_opt.?;
    defer acc_row.deinit();

    const cur_balance = rowGetF64(&acc_row, "balance");
    const new_balance = cur_balance + amount;

    // Update balance
    var upd = std.StringHashMap(Value).init(std.heap.page_allocator);
    defer upd.deinit();
    upd.put("balance", .{ .float = new_balance }) catch {};
    const updated_acc = db.update("accounts", acc_id, &upd) catch null;
    if (updated_acc) |*row_ptr| {
        var row = row_ptr.*;
        row.deinit();
    }

    // Insert transaction
    var tx_data = std.StringHashMap(Value).init(std.heap.page_allocator);
    defer tx_data.deinit();
    tx_data.put("from_account", .{ .uint = 0 }) catch {};
    tx_data.put("to_account", .{ .uint = acc_id }) catch {};
    tx_data.put("amount", .{ .float = amount }) catch {};
    tx_data.put("type", .{ .string = "deposit" }) catch {};
    tx_data.put("description", .{ .string = "Deposit" }) catch {};
    tx_data.put("status", .{ .string = "completed" }) catch {};

    const tx_row = db.insert("transactions", &tx_data) catch {
        sendJson(conn, 500, "{\"error\":\"transaction insert failed\"}");
        return;
    };
    defer {
        var owned = tx_row;
        owned.deinit();
    }

    // Update atomic balance
    const cents: i64 = @intFromFloat(amount * 100);
    _ = total_balance_cents.fetchAdd(cents, .monotonic);

    var id_buf: [32]u8 = undefined;
    const id_str = std.fmt.bufPrint(&id_buf, "{d}", .{tx_row.id}) catch return;

    var jw = JsonWriter.init(std.heap.page_allocator);
    defer jw.deinit();
    jw.objectStart() catch return;
    jw.writeKey("data") catch return;
    jw.objectStart() catch return;
    jw.writeKey("id") catch return;
    jw.writeString(id_str) catch return;
    jw.writeKey("status") catch return;
    jw.writeString("completed") catch return;
    jw.writeKey("balance") catch return;
    jw.writeFloat(new_balance) catch return;
    jw.objectEnd() catch return;
    jw.objectEnd() catch return;

    const resp = jw.toOwnedSlice() catch return;
    defer std.heap.page_allocator.free(resp);
    sendJson(conn, 200, resp);
}

fn handleTransfer(conn: std.net.Stream, uid: u64, req: *const HttpRequest) void {
    _ = uid;
    var body_map = parseJsonBody(req.body);
    defer body_map.deinit();

    const from_id = jsonID(&body_map, "fromAccountId");
    const to_id = jsonID(&body_map, "toAccountId");
    const amount = jsonF64(&body_map, "amount");
    const desc = jsonStr(&body_map, "description");

    if (from_id == 0 or to_id == 0 or amount <= 0) {
        sendJson(conn, 400, "{\"error\":\"invalid transfer params\"}");
        return;
    }

    const accounts_tbl = db.table("accounts") orelse {
        sendJson(conn, 404, "{\"error\":\"account not found\"}");
        return;
    };

    const from_opt = accounts_tbl.get(from_id) catch null;
    const to_opt = accounts_tbl.get(to_id) catch null;

    if (from_opt == null or to_opt == null) {
        sendJson(conn, 404, "{\"error\":\"account not found\"}");
        return;
    }

    var from_row = from_opt.?;
    defer from_row.deinit();
    var to_row = to_opt.?;
    defer to_row.deinit();

    const from_balance = rowGetF64(&from_row, "balance");

    if (from_balance < amount) {
        // Insufficient funds — record as failed
        var tx_data = std.StringHashMap(Value).init(std.heap.page_allocator);
        defer tx_data.deinit();
        tx_data.put("from_account", .{ .uint = from_id }) catch {};
        tx_data.put("to_account", .{ .uint = to_id }) catch {};
        tx_data.put("amount", .{ .float = amount }) catch {};
        tx_data.put("type", .{ .string = "transfer" }) catch {};
        tx_data.put("description", .{ .string = desc }) catch {};
        tx_data.put("status", .{ .string = "failed" }) catch {};

        const tx_row = db.insert("transactions", &tx_data) catch {
            sendJson(conn, 500, "{\"error\":\"transaction insert failed\"}");
            return;
        };
        defer {
            var owned = tx_row;
            owned.deinit();
        }

        var id_buf: [32]u8 = undefined;
        const id_str = std.fmt.bufPrint(&id_buf, "{d}", .{tx_row.id}) catch return;

        var jw = JsonWriter.init(std.heap.page_allocator);
        defer jw.deinit();
        jw.objectStart() catch return;
        jw.writeKey("data") catch return;
        jw.objectStart() catch return;
        jw.writeKey("id") catch return;
        jw.writeString(id_str) catch return;
        jw.writeKey("status") catch return;
        jw.writeString("failed") catch return;
        jw.writeKey("reason") catch return;
        jw.writeString("insufficient_funds") catch return;
        jw.objectEnd() catch return;
        jw.objectEnd() catch return;

        const resp = jw.toOwnedSlice() catch return;
        defer std.heap.page_allocator.free(resp);
        sendJson(conn, 200, resp);
        return;
    }

    // Debit + credit
    var upd_from = std.StringHashMap(Value).init(std.heap.page_allocator);
    defer upd_from.deinit();
    upd_from.put("balance", .{ .float = from_balance - amount }) catch {};
    const updated_from = db.update("accounts", from_id, &upd_from) catch null;
    if (updated_from) |*row_ptr| {
        var row = row_ptr.*;
        row.deinit();
    }

    const to_balance = rowGetF64(&to_row, "balance");
    var upd_to = std.StringHashMap(Value).init(std.heap.page_allocator);
    defer upd_to.deinit();
    upd_to.put("balance", .{ .float = to_balance + amount }) catch {};
    const updated_to = db.update("accounts", to_id, &upd_to) catch null;
    if (updated_to) |*row_ptr| {
        var row = row_ptr.*;
        row.deinit();
    }

    // Insert transaction
    var tx_data = std.StringHashMap(Value).init(std.heap.page_allocator);
    defer tx_data.deinit();
    tx_data.put("from_account", .{ .uint = from_id }) catch {};
    tx_data.put("to_account", .{ .uint = to_id }) catch {};
    tx_data.put("amount", .{ .float = amount }) catch {};
    tx_data.put("type", .{ .string = "transfer" }) catch {};
    tx_data.put("description", .{ .string = desc }) catch {};
    tx_data.put("status", .{ .string = "completed" }) catch {};

    const tx_row = db.insert("transactions", &tx_data) catch {
        sendJson(conn, 500, "{\"error\":\"transaction insert failed\"}");
        return;
    };
    defer {
        var owned = tx_row;
        owned.deinit();
    }

    var id_buf: [32]u8 = undefined;
    const id_str = std.fmt.bufPrint(&id_buf, "{d}", .{tx_row.id}) catch return;

    var jw = JsonWriter.init(std.heap.page_allocator);
    defer jw.deinit();
    jw.objectStart() catch return;
    jw.writeKey("data") catch return;
    jw.objectStart() catch return;
    jw.writeKey("id") catch return;
    jw.writeString(id_str) catch return;
    jw.writeKey("status") catch return;
    jw.writeString("completed") catch return;
    jw.objectEnd() catch return;
    jw.objectEnd() catch return;

    const resp = jw.toOwnedSlice() catch return;
    defer std.heap.page_allocator.free(resp);
    sendJson(conn, 200, resp);
}

// ─── View handlers ──────────────────────────────────────────────────────────

fn handleView(conn: std.net.Stream, req: *const HttpRequest) void {
    const prefix = "/api/view/";
    if (req.path.len <= prefix.len) {
        sendJson(conn, 404, "{\"error\":\"unknown view\"}");
        return;
    }
    const name = req.path[prefix.len..];

    if (std.mem.eql(u8, name, "get_stats")) {
        handleGetStats(conn);
    } else if (std.mem.eql(u8, name, "get_all_accounts")) {
        handleGetAllAccounts(conn, req);
    } else if (std.mem.eql(u8, name, "get_recent_transactions")) {
        handleGetRecentTransactions(conn, req);
    } else if (std.mem.eql(u8, name, "get_transactions")) {
        handleGetTransactions(conn, req);
    } else {
        var err_buf: [128]u8 = undefined;
        const err_msg = std.fmt.bufPrint(&err_buf, "{{\"error\":\"unknown view: {s}\"}}", .{name}) catch return;
        sendJson(conn, 404, err_msg);
    }
}

fn handleGetStats(conn: std.net.Stream) void {
    const users_tbl = db.table("users");
    const acc_tbl = db.table("accounts");
    const tx_tbl = db.table("transactions");

    const user_count: i64 = if (users_tbl) |t| t.getCount() else 0;
    const acc_count: i64 = if (acc_tbl) |t| t.getCount() else 0;
    const tx_count: i64 = if (tx_tbl) |t| t.getCount() else 0;

    const balance_cents = total_balance_cents.load(.monotonic);
    const total_balance_f: f64 = @round(@as(f64, @floatFromInt(balance_cents)) / 100.0);

    var jw = JsonWriter.init(std.heap.page_allocator);
    defer jw.deinit();
    jw.objectStart() catch return;
    jw.writeKey("data") catch return;
    jw.objectStart() catch return;
    jw.writeKey("users") catch return;
    jw.writeInt(user_count) catch return;
    jw.writeKey("accounts") catch return;
    jw.writeInt(acc_count) catch return;
    jw.writeKey("transactions") catch return;
    jw.writeInt(tx_count) catch return;
    jw.writeKey("totalBalance") catch return;
    jw.writeFloat(total_balance_f) catch return;
    jw.objectEnd() catch return;
    jw.objectEnd() catch return;

    const resp = jw.toOwnedSlice() catch return;
    defer std.heap.page_allocator.free(resp);
    sendJson(conn, 200, resp);
}

fn handleGetAllAccounts(conn: std.net.Stream, req: *const HttpRequest) void {
    const uid = authFromHeader(req.auth_header);

    const accounts_tbl = db.table("accounts") orelse {
        sendJson(conn, 200, "{\"data\":[]}");
        return;
    };

    var jw = JsonWriter.init(std.heap.page_allocator);
    defer jw.deinit();
    jw.objectStart() catch return;
    jw.writeKey("data") catch return;
    jw.arrayStart() catch return;

    tl_accounts_writer = &jw;
    defer tl_accounts_writer = null;

    if (uid != 0) {
        accounts_tbl.scanByField("owner_id", .{ .uint = uid }, &struct {
            fn cb(row: *Row) bool {
                if (tl_accounts_writer) |writer| {
                    writeAccountJson(writer, row);
                }
                return true;
            }
        }.cb) catch {};
    } else {
        accounts_tbl.scan(&struct {
            fn cb(row: *Row) bool {
                if (tl_accounts_writer) |writer| {
                    writeAccountJson(writer, row);
                }
                return true;
            }
        }.cb) catch {};
    }

    jw.arrayEnd() catch return;
    jw.objectEnd() catch return;

    const resp = jw.toOwnedSlice() catch return;
    defer std.heap.page_allocator.free(resp);
    sendJson(conn, 200, resp);
}

fn handleGetRecentTransactions(conn: std.net.Stream, req: *const HttpRequest) void {
    var limit: usize = 50;
    if (getQueryParam(req.query, "limit")) |limit_str| {
        limit = std.fmt.parseInt(usize, limit_str, 10) catch 50;
    }

    const tx_tbl = db.table("transactions") orelse {
        sendJson(conn, 200, "{\"data\":[]}");
        return;
    };

    var jw = JsonWriter.init(std.heap.page_allocator);
    defer jw.deinit();
    jw.objectStart() catch return;
    jw.writeKey("data") catch return;
    jw.arrayStart() catch return;

    // Scan last N transactions (reverse order)
    tx_tbl.mutex.lock();
    defer tx_tbl.mutex.unlock();

    // Collect all page IDs
    var page_ids = std.array_list.Managed(u64).init(std.heap.page_allocator);
    defer page_ids.deinit();

    var page_id = tx_tbl.pager.root_page_id;
    while (page_id != 0) {
        page_ids.append(page_id) catch break;
        const page = tx_tbl.pager.readPage(page_id) catch break;
        page_id = page.overflowId();
    }

    var count: usize = 0;
    var pi: usize = page_ids.items.len;
    while (pi > 0 and count < limit) {
        pi -= 1;
        const page = tx_tbl.pager.readPage(page_ids.items[pi]) catch break;
        const n = page.numEntries();
        var i: usize = n;
        while (i > 0 and count < limit) {
            i -= 1;
            if (page.entryAt(@intCast(i))) |entry| {
                var row = decodeRowBasic(entry.value) orelse continue;
                defer row.deinit();
                writeTxJson(&jw, &row);
                count += 1;
            }
        }
    }

    jw.arrayEnd() catch return;
    jw.objectEnd() catch return;

    const resp = jw.toOwnedSlice() catch return;
    defer std.heap.page_allocator.free(resp);
    sendJson(conn, 200, resp);
}

fn handleGetTransactions(conn: std.net.Stream, req: *const HttpRequest) void {
    const acc_id_str = getQueryParam(req.query, "accountId") orelse {
        sendJson(conn, 200, "{\"data\":[]}");
        return;
    };
    const acc_id = std.fmt.parseInt(u64, acc_id_str, 10) catch 0;
    if (acc_id == 0) {
        sendJson(conn, 200, "{\"data\":[]}");
        return;
    }

    const tx_tbl = db.table("transactions") orelse {
        sendJson(conn, 200, "{\"data\":[]}");
        return;
    };

    var jw = JsonWriter.init(std.heap.page_allocator);
    defer jw.deinit();
    jw.objectStart() catch return;
    jw.writeKey("data") catch return;
    jw.arrayStart() catch return;

    // Track seen IDs to deduplicate
    var seen = std.AutoHashMap(u64, void).init(std.heap.page_allocator);
    defer seen.deinit();

    tl_tx_writer = &jw;
    tl_tx_seen = &seen;
    defer {
        tl_tx_writer = null;
        tl_tx_seen = null;
    }

    tx_tbl.scanByField("from_account", .{ .uint = acc_id }, &struct {
        fn cb(row: *Row) bool {
            if (tl_tx_seen) |seen_map| {
                if (seen_map.get(row.id) == null) {
                    seen_map.put(row.id, {}) catch {};
                    if (tl_tx_writer) |writer| writeTxJson(writer, row);
                }
            }
            return true;
        }
    }.cb) catch {};

    tx_tbl.scanByField("to_account", .{ .uint = acc_id }, &struct {
        fn cb(row: *Row) bool {
            if (tl_tx_seen) |seen_map| {
                if (seen_map.get(row.id) == null) {
                    seen_map.put(row.id, {}) catch {};
                    if (tl_tx_writer) |writer| writeTxJson(writer, row);
                }
            }
            return true;
        }
    }.cb) catch {};

    jw.arrayEnd() catch return;
    jw.objectEnd() catch return;

    const resp = jw.toOwnedSlice() catch return;
    defer std.heap.page_allocator.free(resp);
    sendJson(conn, 200, resp);
}

fn writeTxJson(jw: *JsonWriter, row: *Row) void {
    var id_buf: [32]u8 = undefined;
    const id_str = std.fmt.bufPrint(&id_buf, "{d}", .{row.id}) catch return;

    const from_acc = rowGetU64(row, "from_account");
    const to_acc = rowGetU64(row, "to_account");
    const amount_val = rowGetF64(row, "amount");

    var from_buf: [32]u8 = undefined;
    const from_str = std.fmt.bufPrint(&from_buf, "{d}", .{from_acc}) catch return;

    var to_buf: [32]u8 = undefined;
    const to_str = std.fmt.bufPrint(&to_buf, "{d}", .{to_acc}) catch return;

    jw.objectStart() catch return;
    jw.writeKey("id") catch return;
    jw.writeString(id_str) catch return;
    jw.writeKey("fromAccount") catch return;
    jw.writeString(from_str) catch return;
    jw.writeKey("toAccount") catch return;
    jw.writeString(to_str) catch return;
    jw.writeKey("amount") catch return;
    jw.writeFloat(amount_val) catch return;
    jw.writeKey("type") catch return;
    jw.writeString(rowGetStr(row, "type")) catch return;
    jw.writeKey("description") catch return;
    jw.writeString(rowGetStr(row, "description")) catch return;
    jw.writeKey("status") catch return;
    jw.writeString(rowGetStr(row, "status")) catch return;
    jw.objectEnd() catch return;
    jw.comma() catch return;
}

threadlocal var tl_accounts_writer: ?*JsonWriter = null;
threadlocal var tl_tx_writer: ?*JsonWriter = null;
threadlocal var tl_tx_seen: ?*std.AutoHashMap(u64, void) = null;

fn writeAccountJson(jw: *JsonWriter, row: *Row) void {
    var id_buf: [32]u8 = undefined;
    const id_str = std.fmt.bufPrint(&id_buf, "{d}", .{row.id}) catch return;
    const balance = rowGetF64(row, "balance");

    jw.objectStart() catch return;
    jw.writeKey("id") catch return;
    jw.writeString(id_str) catch return;
    jw.writeKey("name") catch return;
    jw.writeString(rowGetStr(row, "name")) catch return;
    jw.writeKey("type") catch return;
    jw.writeString(rowGetStr(row, "type")) catch return;
    jw.writeKey("currency") catch return;
    jw.writeString(rowGetStr(row, "currency")) catch return;
    jw.writeKey("balance") catch return;
    jw.writeFloat(balance) catch return;
    jw.objectEnd() catch return;
    jw.comma() catch return;
}

// ─── Main ───────────────────────────────────────────────────────────────────

var server_instance: ?*std.net.Server = null;
var shutdown_requested: std.atomic.Value(bool) = std.atomic.Value(bool).init(false);

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    _ = &gpa;
    const allocator = std.heap.page_allocator;

    // Parse args
    var port: u16 = 1985;
    var data_dir: []const u8 = "benchmarks/finance-zig/data";

    var args = std.process.args();
    _ = args.skip(); // skip program name
    while (args.next()) |a| {
        if (std.mem.startsWith(u8, a, "--port=")) {
            port = std.fmt.parseInt(u16, a[7..], 10) catch 1985;
        } else if (std.mem.startsWith(u8, a, "--data=")) {
            data_dir = a[7..];
        }
    }

    // Open database
    db = try Database.open(allocator, data_dir);

    // Create tables
    _ = try db.createTable(&user_schema);
    _ = try db.createTable(&account_schema);
    _ = try db.createTable(&transaction_schema);

    // Initialize auth
    auth = Auth.init(allocator);

    // Set up SIGINT handler for graceful shutdown
    const sigaction = std.posix.Sigaction{
        .handler = .{ .handler = struct {
            fn handler(_: c_int) callconv(.c) void {
                shutdown_requested.store(true, .release);
                // Close the listening socket to unblock accept
                if (server_instance) |srv| {
                    srv.deinit();
                    server_instance = null;
                }
            }
        }.handler },
        .mask = std.posix.sigemptyset(),
        .flags = 0,
    };
    std.posix.sigaction(std.posix.SIG.INT, &sigaction, null);

    const address = std.net.Address.parseIp("0.0.0.0", port) catch unreachable;
    var server = try address.listen(.{
        .reuse_address = true,
    });
    server_instance = &server;

    std.debug.print("flop-zig finance server on :{d} (data={s})\n", .{ port, data_dir });

    // Accept loop with thread pool
    while (!shutdown_requested.load(.acquire)) {
        const conn = server.accept() catch |err| {
            if (shutdown_requested.load(.acquire)) break;
            if (err == error.SocketNotListening) break;
            continue;
        };

        // Spawn thread for each connection
        const thread = std.Thread.spawn(.{}, handleConnectionWrapper, .{conn.stream}) catch {
            conn.stream.close();
            continue;
        };
        thread.detach();
    }

    // Graceful shutdown
    std.debug.print("shutting down...\n", .{});
    db.flush() catch {};
    db.close();
    auth.deinit();
}

fn handleConnectionWrapper(conn: std.net.Stream) void {
    handleConnection(conn);
}
