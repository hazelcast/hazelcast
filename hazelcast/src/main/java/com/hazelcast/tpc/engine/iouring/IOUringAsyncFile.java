package com.hazelcast.tpc.engine.iouring;


import com.hazelcast.tpc.engine.AsyncFile;
import com.hazelcast.tpc.engine.Eventloop;
import com.hazelcast.tpc.engine.Promise;
import io.netty.incubator.channel.uring.Native;
import net.smacke.jaydio.DirectIoLib;

import static io.netty.incubator.channel.uring.Native.IORING_OP_CLOSE;
import static io.netty.incubator.channel.uring.Native.IORING_OP_FALLOCATE;
import static io.netty.incubator.channel.uring.Native.IORING_OP_FSYNC;
import static io.netty.incubator.channel.uring.Native.IORING_OP_NOP;
import static io.netty.incubator.channel.uring.Native.IORING_OP_READ;
import static io.netty.incubator.channel.uring.Native.IORING_OP_WRITE;

/**
 * IOUring implementation of the {@link AsyncFile}.
 */
public final class IOUringAsyncFile extends AsyncFile {

    private final IOUringEventloop eventloop;
    private final IORequestScheduler ioRequestScheduler;
    private final Eventloop.Unsafe unsafe;
    private final String path;

    IOUringAsyncFile(String path, IOUringEventloop eventloop) {
        this.path = path;
        this.eventloop = eventloop;
        this.unsafe = eventloop.unsafe();
        this.ioRequestScheduler = eventloop.ioRequestScheduler;
    }

    @Override
    public String path() {
        return path;
    }

    @Override
    public Promise nop() {
        return ioRequestScheduler.schedule(IORING_OP_NOP, 0, 0, fd, 0, 0, 0);
    }

    @Override
    public Promise pread(long offset, int length, long bufferAddress) {
        return ioRequestScheduler.schedule(IORING_OP_READ, 0, 0, fd, bufferAddress, length, offset);
    }

    @Override
    public Promise pwrite(long offset, int length, long bufferAddress) {
        return ioRequestScheduler.schedule(IORING_OP_WRITE, 0, 0, fd, bufferAddress, length, offset);
    }

    @Override
    public Promise fsync() {
        return ioRequestScheduler.schedule(IORING_OP_FSYNC, 0, 0, fd, 0, 0, 0);
    }

    // for mapping see: https://patchwork.kernel.org/project/linux-fsdevel/patch/20191213183632.19441-2-axboe@kernel.dk/
    @Override
    public Promise fallocate(int mode, long offset, long len) {
        return ioRequestScheduler.schedule(IORING_OP_FALLOCATE, 0, 0, fd, len, mode, offset);
    }

    @Override
    public Promise delete() {
        throw new RuntimeException("Not yet implemented");
    }

    // todo: this should be taken care of by io_uring and not DirectIoLib because it is blocking.
    @Override
    public Promise open(int flags) {
        int fd = DirectIoLib.open(path, flags, 644);
        if (fd < 0) {
            throw new RuntimeException("Can't open file [" + path + "]");
        }

        this.fd = fd;
        ioRequestScheduler.register(this);
        return unsafe.newCompletedPromise(null);
    }

    @Override
    public Promise close() {
        return ioRequestScheduler.schedule(IORING_OP_CLOSE, 0, 0, fd, 0, 0, 0);


//        IoRequest ioRequest = storageScheduler.newIORequest();
//        ioRequest.fd = fd;
//        ioRequest.op = IORING_OP_CLOSE;
//        // todo: we also need to take care of deregistering.
//        return storageScheduler.schedule(ioRequest);

    }
}
