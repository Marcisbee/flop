const std = @import("std");
const flop = @import("flop");
const Database = flop.Database.Database;
const table_mod = flop.Table;
const Schema = table_mod.Schema;
const Field = table_mod.Field;
const FieldType = table_mod.FieldType;
const Value = table_mod.Value;
const Row = table_mod.Row;
const fts_mod = flop.Fts;
const encoding = flop.Encoding;

const movies_schema = Schema{
    .name = "movies",
    .fields = &[_]Field{
        .{ .name = "slug", .field_type = .string, .required = true, .unique = true },
        .{ .name = "title", .field_type = .string, .required = true, .searchable = true },
        .{ .name = "year", .field_type = .int, .required = true, .indexed = true },
        .{ .name = "runtime_minutes", .field_type = .int },
        .{ .name = "rating", .field_type = .float },
        .{ .name = "votes", .field_type = .int },
        .{ .name = "genres", .field_type = .string },
        .{ .name = "overview", .field_type = .string },
    },
};

var db: Database = undefined;
var db_initialized = false;

const HttpRequest = struct {
    method: []const u8,
    path: []const u8,
    query: []const u8,
    version: []const u8,
    headers: [64]Header,
    header_count: usize,
    body: []const u8,

    const Header = struct {
        name: []const u8,
        value: []const u8,
    };
};

fn parseRequest(buf: []const u8) ?HttpRequest {
    var req = HttpRequest{
        .method = "",
        .path = "",
        .query = "",
        .version = "",
        .headers = undefined,
        .header_count = 0,
        .body = "",
    };

    // Find end of request line
    const line_end = std.mem.indexOf(u8, buf, "\r\n") orelse return null;
    const request_line = buf[0..line_end];

    // Parse method
    const method_end = std.mem.indexOf(u8, request_line, " ") orelse return null;
    req.method = request_line[0..method_end];

    // Parse URI
    const rest_after_method = request_line[method_end + 1 ..];
    const uri_end = std.mem.indexOf(u8, rest_after_method, " ") orelse return null;
    const uri = rest_after_method[0..uri_end];
    req.version = rest_after_method[uri_end + 1 ..];

    // Split path and query
    if (std.mem.indexOf(u8, uri, "?")) |qmark| {
        req.path = uri[0..qmark];
        req.query = uri[qmark + 1 ..];
    } else {
        req.path = uri;
        req.query = "";
    }

    // Parse headers
    var offset = line_end + 2;
    while (offset < buf.len and req.header_count < 64) {
        const hdr_end = std.mem.indexOf(u8, buf[offset..], "\r\n") orelse break;
        if (hdr_end == 0) {
            offset += 2;
            break;
        }
        const hdr_line = buf[offset .. offset + hdr_end];
        if (std.mem.indexOf(u8, hdr_line, ": ")) |colon| {
            req.headers[req.header_count] = .{
                .name = hdr_line[0..colon],
                .value = hdr_line[colon + 2 ..],
            };
            req.header_count += 1;
        }
        offset += hdr_end + 2;
    }

    req.body = buf[offset..];
    return req;
}

fn getQueryParam(query: []const u8, name: []const u8) ?[]const u8 {
    var remaining = query;
    while (remaining.len > 0) {
        const pair_end = std.mem.indexOf(u8, remaining, "&") orelse remaining.len;
        const pair = remaining[0..pair_end];

        if (std.mem.indexOf(u8, pair, "=")) |eq| {
            const key = pair[0..eq];
            const val = pair[eq + 1 ..];
            if (std.mem.eql(u8, key, name)) {
                return val;
            }
        }

        if (pair_end >= remaining.len) break;
        remaining = remaining[pair_end + 1 ..];
    }
    return null;
}

fn sendResponse(stream: std.net.Stream, status: []const u8, content_type: []const u8, body: []const u8) void {
    var header_buf: [1024]u8 = undefined;
    const header = std.fmt.bufPrint(&header_buf, "HTTP/1.1 {s}\r\nContent-Type: {s}\r\nContent-Length: {d}\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n", .{ status, content_type, body.len }) catch return;
    _ = stream.write(header) catch return;
    if (body.len > 0) {
        _ = stream.write(body) catch return;
    }
}

fn sendResponseNoCache(stream: std.net.Stream, status: []const u8, content_type: []const u8, body: []const u8) void {
    var header_buf: [1024]u8 = undefined;
    const header = std.fmt.bufPrint(&header_buf, "HTTP/1.1 {s}\r\nContent-Type: {s}\r\nContent-Length: {d}\r\nCache-Control: no-store, max-age=0\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n", .{ status, content_type, body.len }) catch return;
    _ = stream.write(header) catch return;
    if (body.len > 0) {
        _ = stream.write(body) catch return;
    }
}

fn handleStats(stream: std.net.Stream) void {
    const tbl = db.table("movies") orelse {
        sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"table not found\"}");
        return;
    };

    var buf: [128]u8 = undefined;
    const json_body = std.fmt.bufPrint(&buf, "{{\"total\":{d}}}", .{tbl.getCount()}) catch {
        sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"format error\"}");
        return;
    };
    sendResponse(stream, "200 OK", "application/json", json_body);
}

fn rowToJson(allocator: std.mem.Allocator, row: *Row) ![]u8 {
    var json = std.array_list.Managed(u8).init(allocator);
    errdefer json.deinit();

    try json.appendSlice("{\"id\":");
    var id_buf: [24]u8 = undefined;
    const id_str = std.fmt.bufPrint(&id_buf, "{d}", .{row.id}) catch "0";
    try json.appendSlice(id_str);

    // Iterate over data fields
    var it = row.data.iterator();
    while (it.next()) |entry| {
        const key = entry.key_ptr.*;
        const val = entry.value_ptr.*;

        try json.append(',');
        try json.append('"');
        try json.appendSlice(key);
        try json.appendSlice("\":");

        switch (val) {
            .string => |s| {
                // Check if this field should be decoded as int or float based on schema
                const field_type = getFieldType(key);
                switch (field_type) {
                    .int => {
                        if (s.len == 8) {
                            const int_val = encoding.decodeI64(s[0..8]);
                            var num_buf: [24]u8 = undefined;
                            const num_str = std.fmt.bufPrint(&num_buf, "{d}", .{int_val}) catch "0";
                            try json.appendSlice(num_str);
                        } else {
                            try json.appendSlice("0");
                        }
                    },
                    .float => {
                        if (s.len == 8) {
                            const float_val = encoding.decodeF64(s[0..8]);
                            var num_buf: [32]u8 = undefined;
                            const num_str = std.fmt.bufPrint(&num_buf, "{d:.1}", .{float_val}) catch "0";
                            try json.appendSlice(num_str);
                        } else {
                            try json.appendSlice("0");
                        }
                    },
                    .string => {
                        // Check if it looks like a JSON array (genres field)
                        if (std.mem.eql(u8, key, "genres") and s.len > 0 and s[0] == '[') {
                            try json.appendSlice(s);
                        } else {
                            try writeJsonString(&json, s);
                        }
                    },
                    else => {
                        try writeJsonString(&json, s);
                    },
                }
            },
            .int => |v| {
                var num_buf: [24]u8 = undefined;
                const num_str = std.fmt.bufPrint(&num_buf, "{d}", .{v}) catch "0";
                try json.appendSlice(num_str);
            },
            .float => |v| {
                var num_buf: [32]u8 = undefined;
                const num_str = std.fmt.bufPrint(&num_buf, "{d:.1}", .{v}) catch "0";
                try json.appendSlice(num_str);
            },
            .boolean => |v| {
                try json.appendSlice(if (v) "true" else "false");
            },
            .uint => |v| {
                var num_buf: [24]u8 = undefined;
                const num_str = std.fmt.bufPrint(&num_buf, "{d}", .{v}) catch "0";
                try json.appendSlice(num_str);
            },
            .null_val => {
                try json.appendSlice("null");
            },
        }
    }

    try json.append('}');
    return json.toOwnedSlice();
}

fn getFieldType(name: []const u8) FieldType {
    for (movies_schema.fields) |field| {
        if (std.mem.eql(u8, field.name, name)) {
            return field.field_type;
        }
    }
    return .string;
}

fn writeJsonString(json: *std.array_list.Managed(u8), s: []const u8) !void {
    try json.append('"');
    for (s) |c| {
        switch (c) {
            '"' => try json.appendSlice("\\\""),
            '\\' => try json.appendSlice("\\\\"),
            '\n' => try json.appendSlice("\\n"),
            '\r' => try json.appendSlice("\\r"),
            '\t' => try json.appendSlice("\\t"),
            else => {
                if (c < 0x20) {
                    try json.appendSlice("\\u00");
                    const hex_chars = "0123456789abcdef";
                    try json.append(hex_chars[c >> 4]);
                    try json.append(hex_chars[c & 0xf]);
                } else {
                    try json.append(c);
                }
            },
        }
    }
    try json.append('"');
}

fn handleMovies(allocator: std.mem.Allocator, stream: std.net.Stream, query: []const u8) void {
    const tbl = db.table("movies") orelse {
        sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"table not found\"}");
        return;
    };

    var limit: usize = 36;
    var offset: usize = 0;

    if (getQueryParam(query, "limit")) |l| {
        limit = std.fmt.parseInt(usize, l, 10) catch 36;
        if (limit > 200) limit = 200;
    }
    if (getQueryParam(query, "offset")) |o| {
        offset = std.fmt.parseInt(usize, o, 10) catch 0;
    }

    // Build response JSON
    var resp = std.array_list.Managed(u8).init(allocator);
    defer resp.deinit();

    const actual_total = @as(usize, @intCast(tbl.getCount()));
    resp.appendSlice("{\"data\":[") catch return;

    tl_movies_alloc = allocator;
    tl_movies_resp = &resp;
    tl_movies_offset = offset;
    tl_movies_limit = limit;
    tl_movies_seen = 0;
    tl_movies_emitted = 0;
    defer {
        tl_movies_alloc = null;
        tl_movies_resp = null;
    }

    tbl.scanByIndex("year", true, &struct {
        fn cb(row: *Row) bool {
            if (tl_movies_seen < tl_movies_offset) {
                tl_movies_seen += 1;
                return true;
            }
            if (tl_movies_emitted >= tl_movies_limit) return false;

            if (tl_movies_alloc) |alloc| {
                if (tl_movies_resp) |response| {
                    const json = rowToJson(alloc, row) catch return true;
                    defer alloc.free(json);
                    if (tl_movies_emitted > 0) response.append(',') catch return false;
                    response.appendSlice(json) catch return false;
                    tl_movies_emitted += 1;
                    tl_movies_seen += 1;
                    return true;
                }
            }
            return false;
        }
    }.cb) catch {
        sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"scan failed\"}");
        return;
    };

    resp.appendSlice("],\"total\":") catch return;
    var total_buf: [24]u8 = undefined;
    const total_str = std.fmt.bufPrint(&total_buf, "{d}", .{actual_total}) catch "0";
    resp.appendSlice(total_str) catch return;
    resp.appendSlice(",\"limit\":") catch return;
    var limit_buf: [24]u8 = undefined;
    const limit_str = std.fmt.bufPrint(&limit_buf, "{d}", .{limit}) catch "0";
    resp.appendSlice(limit_str) catch return;
    resp.appendSlice(",\"offset\":") catch return;
    var off_buf: [24]u8 = undefined;
    const off_str = std.fmt.bufPrint(&off_buf, "{d}", .{offset}) catch "0";
    resp.appendSlice(off_str) catch return;
    resp.append('}') catch return;

    sendResponse(stream, "200 OK", "application/json", resp.items);
}

threadlocal var tl_movies_alloc: ?std.mem.Allocator = null;
threadlocal var tl_movies_resp: ?*std.array_list.Managed(u8) = null;
threadlocal var tl_movies_offset: usize = 0;
threadlocal var tl_movies_limit: usize = 0;
threadlocal var tl_movies_seen: usize = 0;
threadlocal var tl_movies_emitted: usize = 0;

// Thread-local for FTS rebuild on startup
threadlocal var tl_fts_db: ?*Database = null;
threadlocal var tl_fts_count: ?*usize = null;

threadlocal var tl_slug_alloc: ?std.mem.Allocator = null;
threadlocal var tl_slug_result: ?[]u8 = null;

fn handleMovieBySlug(allocator: std.mem.Allocator, stream: std.net.Stream, slug: []const u8) void {
    const tbl = db.table("movies") orelse {
        sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"table not found\"}");
        return;
    };

    // Decode percent-encoded slug
    var decoded_buf: [512]u8 = undefined;
    const decoded_slug = percentDecode(slug, &decoded_buf);

    tl_slug_alloc = allocator;
    tl_slug_result = null;
    defer tl_slug_alloc = null;

    tbl.scanByField("slug", .{ .string = decoded_slug }, &struct {
        fn cb(row: *Row) bool {
            if (tl_slug_alloc) |alloc| {
                tl_slug_result = rowToJson(alloc, row) catch null;
            }
            return false;
        }
    }.cb) catch {
        sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"scan failed\"}");
        return;
    };

    if (tl_slug_result) |json| {
        defer allocator.free(json);
        var resp = std.array_list.Managed(u8).init(allocator);
        defer resp.deinit();
        resp.appendSlice("{\"data\":[") catch return;
        resp.appendSlice(json) catch return;
        resp.appendSlice("],\"total\":1}") catch return;
        sendResponse(stream, "200 OK", "application/json", resp.items);
    } else {
        sendResponse(stream, "200 OK", "application/json", "{\"data\":[],\"total\":0}");
    }
}

fn handleSearch(allocator: std.mem.Allocator, stream: std.net.Stream, query: []const u8) void {
    const q_raw = getQueryParam(query, "q") orelse {
        sendResponse(stream, "200 OK", "application/json", "{\"data\":[],\"total\":0}");
        return;
    };

    // Percent-decode the query
    var decoded_buf: [512]u8 = undefined;
    const q = percentDecode(q_raw, &decoded_buf);

    if (q.len == 0) {
        sendResponse(stream, "200 OK", "application/json", "{\"data\":[],\"total\":0}");
        return;
    }

    var limit: usize = 10;
    if (getQueryParam(query, "limit")) |l| {
        limit = std.fmt.parseInt(usize, l, 10) catch 10;
        if (limit > 50) limit = 50;
    }

    const fields = &[_][]const u8{"title"};
    const results = db.search("movies", fields, q, limit) catch {
        sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"search failed\"}");
        return;
    };
    defer allocator.free(results);

    const tbl = db.table("movies") orelse {
        sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"table not found\"}");
        return;
    };

    var resp = std.array_list.Managed(u8).init(allocator);
    defer resp.deinit();
    resp.appendSlice("{\"data\":[") catch return;

    var count: usize = 0;
    for (results) |result| {
        const maybe_row = tbl.get(result.doc_id) catch null;
        if (maybe_row) |*row_ptr| {
            var row = row_ptr.*;
            defer row.deinit();
            const json = rowToJson(allocator, &row) catch continue;
            defer allocator.free(json);
            if (count > 0) resp.append(',') catch return;
            resp.appendSlice(json) catch return;
            count += 1;
        }
    }

    resp.appendSlice("],\"total\":") catch return;
    var total_buf: [24]u8 = undefined;
    const total_str = std.fmt.bufPrint(&total_buf, "{d}", .{count}) catch "0";
    resp.appendSlice(total_str) catch return;
    resp.append('}') catch return;

    sendResponse(stream, "200 OK", "application/json", resp.items);
}

fn handleStaticFile(allocator: std.mem.Allocator, stream: std.net.Stream, path: []const u8) void {
    // Strip /static/ prefix
    const file_path = if (std.mem.startsWith(u8, path, "/static/"))
        path[8..]
    else
        path;

    if (file_path.len == 0 or std.mem.indexOf(u8, file_path, "..") != null) {
        sendResponse(stream, "404 Not Found", "text/plain", "Not Found");
        return;
    }

    // Strip query string from file_path
    const clean_path = if (std.mem.indexOf(u8, file_path, "?")) |qmark|
        file_path[0..qmark]
    else
        file_path;

    var full_path_buf: [1024]u8 = undefined;
    const full_path = std.fmt.bufPrint(&full_path_buf, "web/assets/{s}", .{clean_path}) catch {
        sendResponse(stream, "404 Not Found", "text/plain", "Not Found");
        return;
    };

    const file = std.fs.cwd().openFile(full_path, .{}) catch {
        sendResponse(stream, "404 Not Found", "text/plain", "Not Found");
        return;
    };
    defer file.close();

    const stat = file.stat() catch {
        sendResponse(stream, "500 Internal Server Error", "text/plain", "Error");
        return;
    };
    const size = stat.size;

    const content = allocator.alloc(u8, size) catch {
        sendResponse(stream, "500 Internal Server Error", "text/plain", "Error");
        return;
    };
    defer allocator.free(content);

    const bytes_read = file.readAll(content) catch {
        sendResponse(stream, "500 Internal Server Error", "text/plain", "Error");
        return;
    };

    const content_type = getContentType(clean_path);
    sendResponseNoCache(stream, "200 OK", content_type, content[0..bytes_read]);
}

fn getContentType(path: []const u8) []const u8 {
    if (std.mem.endsWith(u8, path, ".js")) return "application/javascript; charset=utf-8";
    if (std.mem.endsWith(u8, path, ".css")) return "text/css; charset=utf-8";
    if (std.mem.endsWith(u8, path, ".svg")) return "image/svg+xml";
    if (std.mem.endsWith(u8, path, ".html")) return "text/html; charset=utf-8";
    if (std.mem.endsWith(u8, path, ".png")) return "image/png";
    if (std.mem.endsWith(u8, path, ".jpg") or std.mem.endsWith(u8, path, ".jpeg")) return "image/jpeg";
    if (std.mem.endsWith(u8, path, ".json")) return "application/json";
    return "application/octet-stream";
}

fn handleSpaFallback(stream: std.net.Stream) void {
    const shell =
        \\<!doctype html>
        \\<html lang="en">
        \\  <head>
        \\    <meta charset="utf-8" />
        \\    <meta name="viewport" content="width=device-width, initial-scale=1" />
        \\    <title>Movies - Flop DB Demo (Zig)</title>
        \\    <link rel="icon" href="/static/favicon.svg" type="image/svg+xml" />
        \\    <link rel="preconnect" href="https://fonts.googleapis.com" />
        \\    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
        \\    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet" />
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
    sendResponseNoCache(stream, "200 OK", "text/html; charset=utf-8", shell);
}

fn percentDecode(input: []const u8, buf: *[512]u8) []const u8 {
    var out_len: usize = 0;
    var i: usize = 0;
    while (i < input.len and out_len < 512) {
        if (input[i] == '%' and i + 2 < input.len) {
            const hi = hexVal(input[i + 1]);
            const lo = hexVal(input[i + 2]);
            if (hi != null and lo != null) {
                buf[out_len] = (@as(u8, hi.?) << 4) | @as(u8, lo.?);
                out_len += 1;
                i += 3;
                continue;
            }
        }
        if (input[i] == '+') {
            buf[out_len] = ' ';
        } else {
            buf[out_len] = input[i];
        }
        out_len += 1;
        i += 1;
    }
    return buf[0..out_len];
}

fn hexVal(c: u8) ?u4 {
    if (c >= '0' and c <= '9') return @intCast(c - '0');
    if (c >= 'a' and c <= 'f') return @intCast(c - 'a' + 10);
    if (c >= 'A' and c <= 'F') return @intCast(c - 'A' + 10);
    return null;
}

fn handleConnection(stream: std.net.Stream) void {
    defer stream.close();

    var buf: [8192]u8 = undefined;
    var total: usize = 0;

    // Read request
    while (total < buf.len) {
        const n = stream.read(buf[total..]) catch return;
        if (n == 0) return;
        total += n;

        // Check if we have the full headers (look for \r\n\r\n)
        if (std.mem.indexOf(u8, buf[0..total], "\r\n\r\n") != null) break;
    }

    const req = parseRequest(buf[0..total]) orelse return;

    // Allocator for this request
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    // Route the request
    if (std.mem.eql(u8, req.method, "OPTIONS")) {
        sendResponse(stream, "200 OK", "text/plain", "");
        return;
    }

    if (!std.mem.eql(u8, req.method, "GET")) {
        sendResponse(stream, "405 Method Not Allowed", "text/plain", "Method Not Allowed");
        return;
    }

    const path = req.path;

    if (std.mem.eql(u8, path, "/api/stats")) {
        handleStats(stream);
    } else if (std.mem.eql(u8, path, "/api/movies")) {
        handleMovies(allocator, stream, req.query);
    } else if (std.mem.startsWith(u8, path, "/api/movies/by-slug/")) {
        const slug = path[20..];
        handleMovieBySlug(allocator, stream, slug);
    } else if (std.mem.eql(u8, path, "/api/search")) {
        handleSearch(allocator, stream, req.query);
    } else if (std.mem.startsWith(u8, path, "/static/")) {
        handleStaticFile(allocator, stream, path);
    } else {
        // SPA fallback
        handleSpaFallback(stream);
    }
}

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    // Open database
    db = try Database.open(allocator, "data");
    db_initialized = true;
    defer db.close();

    _ = try db.createTable(&movies_schema);

    // Rebuild FTS index from existing data on startup
    std.debug.print("Rebuilding search index...\n", .{});
    if (db.table("movies")) |tbl| {
        var fts_count: usize = 0;
        tl_fts_db = &db;
        tl_fts_count = &fts_count;
        tbl.scan(&struct {
            fn cb(row: *Row) bool {
                if (row.get("title")) |v| {
                    switch (v) {
                        .string => |s| {
                            if (tl_fts_db) |fts_db| {
                                if (fts_db.fts_indexes.get("movies.title")) |fts| {
                                    fts.indexDoc(row.id, s) catch {};
                                    if (tl_fts_count) |cnt| {
                                        cnt.* += 1;
                                    }
                                }
                            }
                        },
                        else => {},
                    }
                }
                return true;
            }
        }.cb) catch {};
        tl_fts_db = null;
        tl_fts_count = null;
        std.debug.print("Indexed {d} movies for search.\n", .{fts_count});
    }

    // Get port from env or default to 3001
    var port: u16 = 3001;
    const port_env = std.posix.getenv("PORT");
    if (port_env) |p| {
        port = std.fmt.parseInt(u16, p, 10) catch 3001;
    }

    const address = std.net.Address.initIp4(.{ 0, 0, 0, 0 }, port);
    var server = try address.listen(.{
        .reuse_address = true,
    });
    defer server.deinit();

    std.debug.print("Movies app (Zig) running on http://localhost:{d}\n", .{port});

    // Accept loop
    while (true) {
        const conn = server.accept() catch |err| {
            std.debug.print("Accept error: {}\n", .{err});
            continue;
        };
        // Handle in a separate thread
        const thread = std.Thread.spawn(.{}, handleConnectionThread, .{conn.stream}) catch {
            conn.stream.close();
            continue;
        };
        thread.detach();
    }
}

fn handleConnectionThread(stream: std.net.Stream) void {
    handleConnection(stream);
}
