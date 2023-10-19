const std = @import("std");

const OUT_FILE = "out.bin";
const BUFFER_SIZE = 4096;

fn readNBytes(allocator: *const std.mem.Allocator, filename: []const u8, n: usize) ![]const u8 {
    const file = try std.fs.cwd().openFile(filename, .{});
    defer file.close();

    var data = try allocator.alloc(u8, n);
    var buf = try allocator.alloc(u8, BUFFER_SIZE);

    var written: usize = 0;
    while (data.len < n) {
        var nwritten = try file.read(buf);
        @memcpy(data[written..], buf[0..nwritten]);
        written += nwritten;
    }

    std.debug.assert(data.len == n);
    return data;
}

const Benchmark = struct {
    t: std.time.Timer,
    file: std.fs.File,
    data: []const u8,
    allocator: *const std.mem.Allocator,

    fn init(
        allocator: *const std.mem.Allocator,
        name: []const u8,
        data: []const u8,
    ) !Benchmark {
        try std.io.getStdOut().writer().print("{s}", .{name});

        var file = try std.fs.cwd().createFile(OUT_FILE, .{
            .truncate = true,
        });

        return Benchmark{
            .t = try std.time.Timer.start(),
            .file = file,
            .data = data,
            .allocator = allocator,
        };
    }

    fn stop(b: *Benchmark) void {
        const s = @as(f64, @floatFromInt(b.t.read())) / std.time.ns_per_s;
        std.io.getStdOut().writer().print(
            ",{d},{d}\n",
            .{ s, @as(f64, @floatFromInt(b.data.len)) / s },
        ) catch unreachable;

        b.file.close();

        var in = readNBytes(b.allocator, OUT_FILE, b.data.len) catch unreachable;
        std.debug.assert(std.mem.eql(u8, in, b.data));
        b.allocator.free(in);
    }
};

pub fn main() !void {
    var allocator = &std.heap.page_allocator;

    const SIZE = 104857600; // 100MiB
    var x = try readNBytes(allocator, "/dev/random", SIZE);
    defer allocator.free(x);

    const RUNS = 10;
    var run: usize = 0;
    while (run < RUNS) : (run += 1) {
        {
            var b = try Benchmark.init(allocator, "blocking", x);
            defer b.stop();

            var i: usize = 0;
            while (i < x.len) : (i += BUFFER_SIZE) {
                const size = @min(BUFFER_SIZE, x.len - i);
                const n = try b.file.write(x[i .. i + size]);
                std.debug.assert(n == size);
            }
        }
    }
}
