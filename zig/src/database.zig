const std = @import("std");
const table_mod = @import("table.zig");
const fts_mod = @import("fts.zig");

pub const Database = struct {
    allocator: std.mem.Allocator,
    dir: []const u8,
    tables: std.StringHashMap(*table_mod.Table),
    fts_indexes: std.StringHashMap(*fts_mod.FtsIndex),
    mutex: std.Thread.Mutex,

    const Self = @This();

    pub fn open(allocator: std.mem.Allocator, dir: []const u8) !Self {
        std.fs.cwd().makeDir(dir) catch |err| {
            if (err != error.PathAlreadyExists) return err;
        };

        return Self{
            .allocator = allocator,
            .dir = dir,
            .tables = std.StringHashMap(*table_mod.Table).init(allocator),
            .fts_indexes = std.StringHashMap(*fts_mod.FtsIndex).init(allocator),
            .mutex = .{},
        };
    }

    pub fn createTable(self: *Self, schema: *const table_mod.Schema) !*table_mod.Table {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.tables.get(schema.name)) |existing| {
            return existing;
        }

        // Build path: dir/tablename.db
        var path_buf: [512]u8 = undefined;
        const path = std.fmt.bufPrint(&path_buf, "{s}/{s}.db", .{ self.dir, schema.name }) catch return error.PathTooLong;

        const tbl = try self.allocator.create(table_mod.Table);
        tbl.* = try table_mod.Table.open(self.allocator, schema, path);
        try self.tables.put(schema.name, tbl);

        // Create FTS index for searchable fields
        for (schema.fields) |field| {
            if (field.searchable) {
                const fts = try self.allocator.create(fts_mod.FtsIndex);
                fts.* = fts_mod.FtsIndex.init(self.allocator);

                var key_buf: [256]u8 = undefined;
                const key = std.fmt.bufPrint(&key_buf, "{s}.{s}", .{ schema.name, field.name }) catch continue;
                const key_owned = try self.allocator.dupe(u8, key);
                try self.fts_indexes.put(key_owned, fts);
            }
        }

        return tbl;
    }

    pub fn table(self: *Self, name: []const u8) ?*table_mod.Table {
        return self.tables.get(name);
    }

    pub fn insert(self: *Self, table_name: []const u8, data: *std.StringHashMap(table_mod.Value)) !table_mod.Row {
        const tbl = self.tables.get(table_name) orelse return error.TableNotFound;
        const row = try tbl.insert(data);

        // Update FTS indexes
        for (tbl.schema.fields) |field| {
            if (field.searchable) {
                if (data.get(field.name)) |val| {
                    switch (val) {
                        .string => |s| {
                            var key_buf: [256]u8 = undefined;
                            const key = std.fmt.bufPrint(&key_buf, "{s}.{s}", .{ table_name, field.name }) catch continue;
                            if (self.fts_indexes.get(key)) |fts| {
                                fts.indexDoc(row.id, s) catch {};
                            }
                        },
                        else => {},
                    }
                }
            }
        }

        return row;
    }

    pub fn update(self: *Self, table_name: []const u8, id: u64, data: *std.StringHashMap(table_mod.Value)) !?table_mod.Row {
        const tbl = self.tables.get(table_name) orelse return error.TableNotFound;
        return try tbl.update(id, data);
    }

    pub fn deleteRow(self: *Self, table_name: []const u8, id: u64) !bool {
        const tbl = self.tables.get(table_name) orelse return error.TableNotFound;
        return try tbl.delete(id);
    }

    pub fn search(self: *Self, table_name: []const u8, fields: []const []const u8, query: []const u8, limit: usize) ![]const fts_mod.FtsIndex.SearchResult {
        for (fields) |field| {
            var key_buf: [256]u8 = undefined;
            const key = std.fmt.bufPrint(&key_buf, "{s}.{s}", .{ table_name, field }) catch continue;
            if (self.fts_indexes.get(key)) |fts| {
                return try fts.search(query, limit);
            }
        }
        return &.{};
    }

    pub fn flush(self: *Self) !void {
        var it = self.tables.valueIterator();
        while (it.next()) |tbl| {
            try tbl.*.flush();
        }
    }

    pub fn close(self: *Self) void {
        self.flush() catch {};
        var it = self.tables.valueIterator();
        while (it.next()) |tbl| {
            tbl.*.close();
            self.allocator.destroy(tbl.*);
        }
        self.tables.deinit();

        var fts_key_it = self.fts_indexes.keyIterator();
        while (fts_key_it.next()) |key| {
            self.allocator.free(key.*);
        }
        var fts_it = self.fts_indexes.valueIterator();
        while (fts_it.next()) |fts| {
            fts.*.deinit();
            self.allocator.destroy(fts.*);
        }
        self.fts_indexes.deinit();
    }
};
