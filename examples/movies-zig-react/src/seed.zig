const std = @import("std");
const flop = @import("flop");

const MoviesSchema = flop.Table.Schema{
    .name = "movies",
    .fields = &[_]flop.Table.Field{
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

const Rating = struct {
    avg: f64,
    votes: i64,
};

const ImportMode = enum {
    auto,
    imdb,
    synthetic,
};

const Config = struct {
    mode: ImportMode = .auto,
    force: bool = false,
    limit: usize = 0,
    synthetic_count: usize = 1000,
    datasets_dir: ?[]const u8 = null,
};

const titles_first = [_][]const u8{
    "The",       "A",        "Dark",     "Lost",      "Last",
    "Final",     "Silent",   "Broken",   "Eternal",   "Hidden",
    "Crimson",   "Midnight", "Golden",   "Forgotten", "Shadow",
    "Iron",      "Savage",   "Frozen",   "Burning",   "Rising",
    "Fallen",    "Secret",   "Distant",  "Ancient",   "Twisted",
    "Shattered", "Hollow",   "Wicked",   "Fading",    "Endless",
};

const titles_second = [_][]const u8{
    "Dreams",    "Horizon",   "Requiem",   "Frontier",  "Legacy",
    "Paradise",  "Whisper",   "Thunder",   "Phantom",   "Eclipse",
    "Prophecy",  "Dominion",  "Vengeance", "Odyssey",   "Kingdom",
    "Inferno",   "Destiny",   "Shadows",   "Eternity",  "Journey",
    "Awakening", "Rebellion", "Redemption", "Empire",   "Fortress",
    "Labyrinth", "Outlaw",    "Storm",     "Heights",   "River",
};

const titles_third = [_][]const u8{
    "of the Damned", "Reloaded",    "Unleashed", "Returns",      "Chronicles",
    "Unbound",       "Resurrected", "Unchained", "Revisited",    "Beyond",
    "Underground",   "in the Dark", "at Dawn",   "of Fire",      "of Ice",
    "Ascending",     "Descending",  "Untold",    "Reborn",       "Forever",
};

const genre_list = [_][]const u8{
    "Action",      "Adventure", "Animation", "Comedy",     "Crime",
    "Drama",       "Fantasy",   "Horror",    "Mystery",    "Romance",
    "Sci-Fi",      "Thriller",  "War",       "Western",    "Documentary",
    "Musical",     "Biography", "History",   "Sport",      "Family",
};

fn randomChoice(comptime items: []const []const u8, rng: std.Random) []const u8 {
    const idx = rng.intRangeAtMost(usize, 0, items.len - 1);
    return items[idx];
}

fn slugify(buf: []u8, title: []const u8) []const u8 {
    var len: usize = 0;
    var prev_dash = false;
    for (title) |c| {
        if (len >= buf.len - 1) break;
        if (std.ascii.isAlphanumeric(c)) {
            buf[len] = std.ascii.toLower(c);
            len += 1;
            prev_dash = false;
        } else if (!prev_dash and len > 0) {
            buf[len] = '-';
            len += 1;
            prev_dash = true;
        }
    }
    if (len > 0 and buf[len - 1] == '-') len -= 1;
    if (len > 80) len = 80;
    return buf[0..len];
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const cfg = try parseArgs(allocator);
    defer freeConfig(allocator, cfg);

    const datasets_dir = try resolveDatasetsDir(allocator, cfg.datasets_dir);
    defer if (datasets_dir.owned) allocator.free(datasets_dir.path);

    if (cfg.force) {
        clearDataDir("data") catch |err| {
            std.debug.print("Warning: failed to clear data dir: {}\n", .{err});
        };
    }

    std.debug.print("Opening database...\n", .{});
    var db_instance = try flop.Database.Database.open(allocator, "data");
    defer db_instance.close();

    _ = try db_instance.createTable(&MoviesSchema);

    const import_mode = blk: {
        if (cfg.mode != .auto) break :blk cfg.mode;
        if (datasets_dir.path.len > 0 and imdbDatasetsPresent(datasets_dir.path)) break :blk .imdb;
        break :blk .synthetic;
    };

    const inserted = switch (import_mode) {
        .imdb => try importImdb(allocator, &db_instance, datasets_dir.path, cfg.limit),
        .synthetic => try importSynthetic(allocator, &db_instance, cfg.synthetic_count),
        .auto => unreachable,
    };

    std.debug.print("Flushing to disk...\n", .{});
    try db_instance.flush();
    std.debug.print("Done! Inserted {d} movies.\n", .{inserted});
}

fn parseArgs(allocator: std.mem.Allocator) !Config {
    var cfg = Config{};
    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    _ = args.next();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--imdb")) {
            cfg.mode = .imdb;
        } else if (std.mem.eql(u8, arg, "--synthetic")) {
            cfg.mode = .synthetic;
        } else if (std.mem.eql(u8, arg, "--force")) {
            cfg.force = true;
        } else if (std.mem.startsWith(u8, arg, "--limit=")) {
            cfg.limit = try std.fmt.parseInt(usize, arg["--limit=".len..], 10);
        } else if (std.mem.startsWith(u8, arg, "--count=")) {
            cfg.synthetic_count = try std.fmt.parseInt(usize, arg["--count=".len..], 10);
        } else if (std.mem.startsWith(u8, arg, "--datasets-dir=")) {
            cfg.datasets_dir = try allocator.dupe(u8, arg["--datasets-dir=".len..]);
        } else if (std.mem.eql(u8, arg, "--help")) {
            printHelp();
            std.process.exit(0);
        } else {
            std.debug.print("Unknown arg: {s}\n", .{arg});
            printHelp();
            return error.InvalidArguments;
        }
    }

    return cfg;
}

fn freeConfig(allocator: std.mem.Allocator, cfg: Config) void {
    if (cfg.datasets_dir) |path| allocator.free(path);
}

fn printHelp() void {
    std.debug.print(
        \\Usage: zig build run-seed -- [--imdb|--synthetic] [--force] [--limit=N] [--count=N] [--datasets-dir=PATH]
        \\  --imdb         Import from IMDb datasets if available
        \\  --synthetic    Generate synthetic sample data
        \\  --force        Delete existing files in ./data before import
        \\  --limit=N      Limit IMDb import to first N movies
        \\  --count=N      Number of synthetic movies to generate
        \\  --datasets-dir=PATH  Directory containing title.basics.tsv.gz and title.ratings.tsv.gz
        \\
    , .{});
}

fn clearDataDir(path: []const u8) !void {
    var dir = try std.fs.cwd().openDir(path, .{ .iterate = true });
    defer dir.close();

    var it = dir.iterate();
    while (try it.next()) |entry| {
        if (std.mem.eql(u8, entry.name, ".gitkeep")) continue;
        switch (entry.kind) {
            .file => try dir.deleteFile(entry.name),
            .directory => try dir.deleteTree(entry.name),
            else => {},
        }
    }
}

const ResolvedPath = struct {
    path: []const u8,
    owned: bool,
};

fn resolveDatasetsDir(allocator: std.mem.Allocator, explicit: ?[]const u8) !ResolvedPath {
    if (explicit) |path| {
        return .{ .path = try allocator.dupe(u8, path), .owned = true };
    }

    const candidates = [_][]const u8{
        "data/_datasets/imdb",
        "../movies-go2-react/data/_datasets/imdb",
    };
    for (candidates) |path| {
        if (imdbDatasetsPresent(path)) {
            return .{ .path = path, .owned = false };
        }
    }

    return .{ .path = "", .owned = false };
}

fn imdbDatasetsPresent(dir_path: []const u8) bool {
    if (dir_path.len == 0) return false;
    var dir = std.fs.cwd().openDir(dir_path, .{}) catch return false;
    defer dir.close();
    dir.access("title.basics.tsv.gz", .{}) catch return false;
    dir.access("title.ratings.tsv.gz", .{}) catch return false;
    return true;
}

fn importSynthetic(allocator: std.mem.Allocator, db_instance: *flop.Database.Database, num_movies: usize) !usize {
    var rng = std.Random.DefaultPrng.init(@as(u64, @intCast(std.time.milliTimestamp())));
    const random = rng.random();

    var inserted: usize = 0;
    var title_buf: [256]u8 = undefined;
    var slug_buf: [300]u8 = undefined;
    var genre_buf: [256]u8 = undefined;
    var id_suffix_buf: [16]u8 = undefined;

    std.debug.print("Generating {d} synthetic movies...\n", .{num_movies});

    for (0..num_movies) |i| {
        const first = randomChoice(&titles_first, random);
        const second = randomChoice(&titles_second, random);

        var title_len: usize = 0;
        const use_third = random.intRangeAtMost(u32, 0, 99) < 40;
        @memcpy(title_buf[title_len .. title_len + first.len], first);
        title_len += first.len;
        title_buf[title_len] = ' ';
        title_len += 1;
        @memcpy(title_buf[title_len .. title_len + second.len], second);
        title_len += second.len;

        if (use_third) {
            const third = randomChoice(&titles_third, random);
            title_buf[title_len] = ' ';
            title_len += 1;
            @memcpy(title_buf[title_len .. title_len + third.len], third);
            title_len += third.len;
        }

        const title = title_buf[0..title_len];
        const base_slug = slugify(&slug_buf, title);
        const id_suffix = std.fmt.bufPrint(&id_suffix_buf, "-{d}", .{i + 1}) catch continue;

        const slug_combined = try allocator.alloc(u8, base_slug.len + id_suffix.len);
        defer allocator.free(slug_combined);
        @memcpy(slug_combined[0..base_slug.len], base_slug);
        @memcpy(slug_combined[base_slug.len..], id_suffix);

        const year_roll = random.intRangeAtMost(u32, 0, 99);
        const year: i64 = if (year_roll < 50)
            random.intRangeAtMost(i64, 2000, 2024)
        else if (year_roll < 80)
            random.intRangeAtMost(i64, 1980, 1999)
        else if (year_roll < 95)
            random.intRangeAtMost(i64, 1960, 1979)
        else
            random.intRangeAtMost(i64, 1920, 1959);

        const runtime: i64 = random.intRangeAtMost(i64, 75, 210);
        const rating: f64 = @as(f64, @floatFromInt(random.intRangeAtMost(u32, 10, 100))) / 10.0;
        const votes: i64 = random.intRangeAtMost(i64, 100, 500000);

        const num_genres = random.intRangeAtMost(usize, 1, 3);
        var genre_indices: [3]usize = undefined;
        var genre_count: usize = 0;
        while (genre_count < num_genres) {
            const idx = random.intRangeAtMost(usize, 0, genre_list.len - 1);
            var dup = false;
            for (genre_indices[0..genre_count]) |gi| {
                if (gi == idx) {
                    dup = true;
                    break;
                }
            }
            if (!dup) {
                genre_indices[genre_count] = idx;
                genre_count += 1;
            }
        }

        var gpos: usize = 0;
        genre_buf[gpos] = '[';
        gpos += 1;
        for (genre_indices[0..genre_count], 0..) |gi, gidx| {
            if (gidx > 0) {
                genre_buf[gpos] = ',';
                gpos += 1;
            }
            genre_buf[gpos] = '"';
            gpos += 1;
            const gname = genre_list[gi];
            @memcpy(genre_buf[gpos .. gpos + gname.len], gname);
            gpos += gname.len;
            genre_buf[gpos] = '"';
            gpos += 1;
        }
        genre_buf[gpos] = ']';
        gpos += 1;

        try insertMovie(db_instance, allocator, .{
            .slug = slug_combined,
            .title = title,
            .year = year,
            .runtime_minutes = runtime,
            .rating = rating,
            .votes = votes,
            .genres = genre_buf[0..gpos],
        });
        inserted += 1;

        if (inserted % 100 == 0) {
            std.debug.print("  Inserted {d} movies...\n", .{inserted});
        }
    }

    return inserted;
}

const MovieInput = struct {
    slug: []const u8,
    title: []const u8,
    year: i64,
    runtime_minutes: ?i64 = null,
    rating: ?f64 = null,
    votes: ?i64 = null,
    genres: []const u8 = "[]",
};

fn insertMovie(db_instance: *flop.Database.Database, allocator: std.mem.Allocator, movie: MovieInput) !void {
    var data = std.StringHashMap(flop.Table.Value).init(allocator);
    defer data.deinit();

    try data.put("slug", .{ .string = movie.slug });
    try data.put("title", .{ .string = movie.title });
    try data.put("year", .{ .int = movie.year });
    try data.put("genres", .{ .string = movie.genres });

    if (movie.runtime_minutes) |runtime| try data.put("runtime_minutes", .{ .int = runtime });
    if (movie.rating) |rating| try data.put("rating", .{ .float = rating });
    if (movie.votes) |votes| try data.put("votes", .{ .int = votes });

    var row = try db_instance.insert("movies", &data);
    row.deinit();
}

fn importImdb(allocator: std.mem.Allocator, db_instance: *flop.Database.Database, datasets_dir: []const u8, limit: usize) !usize {
    if (!imdbDatasetsPresent(datasets_dir)) {
        std.debug.print("IMDb datasets not found in {s}\n", .{datasets_dir});
        return error.FileNotFound;
    }

    var basics_path_buf: [512]u8 = undefined;
    var ratings_path_buf: [512]u8 = undefined;
    const basics_path = try std.fmt.bufPrint(&basics_path_buf, "{s}/title.basics.tsv.gz", .{datasets_dir});
    const ratings_path = try std.fmt.bufPrint(&ratings_path_buf, "{s}/title.ratings.tsv.gz", .{datasets_dir});

    std.debug.print("Loading ratings from {s}...\n", .{ratings_path});
    var ratings = try loadRatings(allocator, ratings_path);
    defer {
        var it = ratings.keyIterator();
        while (it.next()) |key| allocator.free(key.*);
        ratings.deinit();
    }
    std.debug.print("Loaded {d} ratings.\n", .{ratings.count()});

    std.debug.print("Importing movies from {s}...\n", .{basics_path});
    var slug_buf: [256]u8 = undefined;
    var id_suffix_buf: [32]u8 = undefined;
    var genres_buf: [256]u8 = undefined;

    var line_no: usize = 0;
    var inserted: usize = 0;

    try readGzipLines(allocator, basics_path, struct {
        allocator: std.mem.Allocator,
        db_instance: *flop.Database.Database,
        ratings: *std.StringHashMap(Rating),
        limit: usize,
        line_no: *usize,
        inserted: *usize,
        slug_buf: *[256]u8,
        id_suffix_buf: *[32]u8,
        genres_buf: *[256]u8,

        fn handle(self: *@This(), line: []const u8) !bool {
            self.line_no.* += 1;
            if (self.line_no.* == 1) return true; // header
            if (self.limit > 0 and self.inserted.* >= self.limit) return false;

            var fields: [9][]const u8 = undefined;
            if (splitTsv(line, &fields) < 9) return true;

            const tconst = fields[0];
            const title_type = fields[1];
            const title = fields[2];
            const year_str = fields[5];
            const runtime_str = fields[7];
            const genres_str = fields[8];

            if (!std.mem.eql(u8, title_type, "movie")) return true;
            if (title.len == 0 or std.mem.eql(u8, title, "\\N")) return true;
            if (year_str.len == 0 or std.mem.eql(u8, year_str, "\\N")) return true;

            const year = std.fmt.parseInt(i64, year_str, 10) catch return true;
            const base_slug = slugify(self.slug_buf, title);
            const suffix = std.fmt.bufPrint(self.id_suffix_buf, "-{s}", .{tconst}) catch return true;
            const slug = try self.allocator.alloc(u8, base_slug.len + suffix.len);
            defer self.allocator.free(slug);
            @memcpy(slug[0..base_slug.len], base_slug);
            @memcpy(slug[base_slug.len..], suffix);

            const genres_json = parseGenres(genres_str, self.genres_buf);
            const runtime = if (!std.mem.eql(u8, runtime_str, "\\N"))
                std.fmt.parseInt(i64, runtime_str, 10) catch null
            else
                null;

            const rating = self.ratings.get(tconst);

            insertMovie(self.db_instance, self.allocator, .{
                .slug = slug,
                .title = title,
                .year = year,
                .runtime_minutes = runtime,
                .rating = if (rating) |r| r.avg else null,
                .votes = if (rating) |r| r.votes else null,
                .genres = genres_json,
            }) catch |err| {
                if (err == error.RowTooLarge) {
                    std.debug.print("Skipping oversized row: {s}\n", .{title});
                    return true;
                }
                return err;
            };

            self.inserted.* += 1;
            if (self.inserted.* % 2000 == 0) {
                try self.db_instance.flush();
                std.debug.print("  Imported {d} movies...\n", .{self.inserted.*});
            }
            return true;
        }
    }{
        .allocator = allocator,
        .db_instance = db_instance,
        .ratings = &ratings,
        .limit = limit,
        .line_no = &line_no,
        .inserted = &inserted,
        .slug_buf = &slug_buf,
        .id_suffix_buf = &id_suffix_buf,
        .genres_buf = &genres_buf,
    });

    return inserted;
}

fn loadRatings(allocator: std.mem.Allocator, ratings_path: []const u8) !std.StringHashMap(Rating) {
    var ratings = std.StringHashMap(Rating).init(allocator);
    var line_no: usize = 0;

    try readGzipLines(allocator, ratings_path, struct {
        allocator: std.mem.Allocator,
        ratings: *std.StringHashMap(Rating),
        line_no: *usize,

        fn handle(self: *@This(), line: []const u8) !bool {
            self.line_no.* += 1;
            if (self.line_no.* == 1) return true;

            var fields: [3][]const u8 = undefined;
            if (splitTsv(line, &fields) < 3) return true;

            const avg = std.fmt.parseFloat(f64, fields[1]) catch return true;
            const votes = std.fmt.parseInt(i64, fields[2], 10) catch return true;
            const key = try self.allocator.dupe(u8, fields[0]);
            try self.ratings.put(key, .{ .avg = avg, .votes = votes });
            return true;
        }
    }{
        .allocator = allocator,
        .ratings = &ratings,
        .line_no = &line_no,
    });

    return ratings;
}

fn readGzipLines(allocator: std.mem.Allocator, path: []const u8, handler: anytype) !void {
    var state = handler;
    var child = std.process.Child.init(&[_][]const u8{ "gzip", "-dc", path }, allocator);
    child.stdout_behavior = .Pipe;
    child.stderr_behavior = .Inherit;
    try child.spawn();

    const stdout = child.stdout.?;

    var line_buf = std.array_list.Managed(u8).init(allocator);
    defer line_buf.deinit();
    var read_buf: [64 * 1024]u8 = undefined;
    var stopped_early = false;

    outer: while (true) {
        const n = try stdout.read(&read_buf);
        if (n == 0) break;
        for (read_buf[0..n]) |byte| {
            if (byte == '\n') {
                const line = std.mem.trimRight(u8, line_buf.items, "\r");
                const keep_going = try state.handle(line);
                if (!keep_going) {
                    stopped_early = true;
                    break :outer;
                }
                line_buf.clearRetainingCapacity();
            } else {
                try line_buf.append(byte);
            }
        }
    }

    if (line_buf.items.len > 0) {
        const line = std.mem.trimRight(u8, line_buf.items, "\r");
        _ = try state.handle(line);
    }

    if (stopped_early) {
        _ = child.kill() catch {};
    }
    const term = try child.wait();
    switch (term) {
        .Exited => |code| if (code != 0 and !stopped_early) return error.InvalidGzipStream,
        .Signal => if (!stopped_early) return error.InvalidGzipStream,
        else => return error.InvalidGzipStream,
    }
}

fn splitTsv(line: []const u8, out: [][]const u8) usize {
    var count: usize = 0;
    var start: usize = 0;
    for (line, 0..) |c, i| {
        if (c == '\t') {
            if (count < out.len) out[count] = line[start..i];
            count += 1;
            start = i + 1;
        }
    }
    if (count < out.len) out[count] = line[start..];
    return count + 1;
}

fn parseGenres(raw: []const u8, buf: *[256]u8) []const u8 {
    if (raw.len == 0 or std.mem.eql(u8, raw, "\\N")) return "[]";

    var out: usize = 0;
    buf[out] = '[';
    out += 1;

    var start: usize = 0;
    var idx: usize = 0;
    while (idx <= raw.len) : (idx += 1) {
        if (idx == raw.len or raw[idx] == ',') {
            if (out + 3 >= buf.len) break;
            if (start != 0) {
                buf[out] = ',';
                out += 1;
            }
            buf[out] = '"';
            out += 1;
            const part = raw[start..idx];
            const copy_len = @min(part.len, buf.len - out - 2);
            @memcpy(buf[out .. out + copy_len], part[0..copy_len]);
            out += copy_len;
            buf[out] = '"';
            out += 1;
            start = idx + 1;
        }
    }

    buf[out] = ']';
    out += 1;
    return buf[0..out];
}
