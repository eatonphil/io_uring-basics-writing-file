const std = @import("std");

const OUT_FILE = "out.bin";
const BUFFER_SIZE: u64 = 1048576; // 4096;

fn readNBytes(
    allocator: *const std.mem.Allocator,
    filename: []const u8,
    n: usize,
) ![]const u8 {
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

fn benchmarkIOUringNEntries(
    allocator: *const std.mem.Allocator,
    data: []const u8,
    nEntries: u13,
) !void {
    const name = try std.fmt.allocPrint(allocator.*, "iouring_{}_entries", .{nEntries});
    defer allocator.free(name);

    var b = try Benchmark.init(allocator, name, data);
    defer b.stop();

    var ring = try std.os.linux.IO_Uring.init(nEntries, 0);
    defer ring.deinit();

    var cqes = try allocator.alloc(std.os.linux.io_uring_cqe, nEntries);
    defer allocator.free(cqes);

    var written: usize = 0;
    var i: usize = 0;
    while (i < data.len or written < data.len) {
        var submittedEntries: u32 = 0;
        var j: usize = 0;
        while (true) {
            const base = i + j * BUFFER_SIZE;
            if (base >= data.len) {
                break;
            }
            const size = @min(BUFFER_SIZE, data.len - base);
            _ = ring.write(0, b.file.handle, data[base .. base + size], base) catch |e| switch (e) {
                error.SubmissionQueueFull => break,
                else => unreachable,
            };
            submittedEntries += 1;
            i += size;
        }

        _ = try ring.submit_and_wait(0);
        const cqesDone = try ring.copy_cqes(cqes, 0);

        for (cqes[0..cqesDone]) |*cqe| {
            std.debug.assert(cqe.err() == .SUCCESS);
            std.debug.assert(cqe.res >= 0);
            const n = @as(usize, @intCast(cqe.res));
            std.debug.assert(n <= BUFFER_SIZE);
            written += n;
        }
    }
}

pub fn main() !void {
    var allocator = &std.heap.page_allocator;

    const SIZE = 1073741824; //104857600; // 100MiB
    var data = try readNBytes(allocator, "/dev/random", SIZE);
    defer allocator.free(data);

    const RUNS = 10;
    var run: usize = 0;
    while (run < RUNS) : (run += 1) {
        {
            var b = try Benchmark.init(allocator, "blocking", data);
            defer b.stop();

            var i: usize = 0;
            while (i < data.len) : (i += BUFFER_SIZE) {
                const size = @min(BUFFER_SIZE, data.len - i);
                const n = try b.file.write(data[i .. i + size]);
                std.debug.assert(n == size);
            }
        }

        try benchmarkIOUringNEntries(allocator, data, 1);
        try benchmarkIOUringNEntries(allocator, data, 128);
    }
}
