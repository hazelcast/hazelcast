package io.netty.incubator.channel.uring;

import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.engine.AsyncSocket;
import com.hazelcast.spi.impl.engine.Eventloop;
import com.hazelcast.spi.impl.engine.Scheduler;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.unix.FileDescriptor;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;
import io.netty.util.internal.PlatformDependent;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;

import static io.netty.incubator.channel.uring.Native.DEFAULT_IOSEQ_ASYNC_THRESHOLD;
import static io.netty.incubator.channel.uring.Native.DEFAULT_RING_SIZE;


/**
 * To build io uring:
 * <p>
 * sudo yum install autoconf
 * sudo yum install automake
 * sudo yum install libtool
 * <p>
 * Good read:
 * https://unixism.net/2020/04/io-uring-by-example-part-3-a-web-server-with-io-uring/
 * <p>
 * Another example (blocking socket)
 * https://github.com/ddeka0/AsyncIO/blob/master/src/asyncServer.cpp
 * <p>
 * no syscalls:
 * https://wjwh.eu/posts/2021-10-01-no-syscall-server-iouring.html
 * <p>
 * Error codes:
 * https://www.thegeekstuff.com/2010/10/linux-error-codes/
 * <p>
 * <p>
 * https://github.com/torvalds/linux/blob/master/include/uapi/linux/io_uring.h
 * IORING_OP_NOP               0
 * IORING_OP_READV             1
 * IORING_OP_WRITEV            2
 * IORING_OP_FSYNC             3
 * IORING_OP_READ_FIXED        4
 * IORING_OP_WRITE_FIXED       5
 * IORING_OP_POLL_ADD          6
 * IORING_OP_POLL_REMOVE       7
 * IORING_OP_SYNC_FILE_RANGE   8
 * IORING_OP_SENDMSG           9
 * IORING_OP_RECVMSG           10
 * IORING_OP_TIMEOUT,          11
 * IORING_OP_TIMEOUT_REMOVE,   12
 * IORING_OP_ACCEPT,           13
 * IORING_OP_ASYNC_CANCEL,     14
 * IORING_OP_LINK_TIMEOUT,     15
 * IORING_OP_CONNECT,          16
 * IORING_OP_FALLOCATE,        17
 * IORING_OP_OPENAT,
 * IORING_OP_CLOSE,
 * IORING_OP_FILES_UPDATE,
 * IORING_OP_STATX,
 * IORING_OP_READ,
 * IORING_OP_WRITE,
 * IORING_OP_FADVISE,
 * IORING_OP_MADVISE,
 * IORING_OP_SEND,
 * IORING_OP_RECV,
 * IORING_OP_OPENAT2,
 * IORING_OP_EPOLL_CTL,
 * IORING_OP_SPLICE,
 * IORING_OP_PROVIDE_BUFFERS,
 * IORING_OP_REMOVE_BUFFERS,
 * IORING_OP_TEE,
 * IORING_OP_SHUTDOWN,
 * IORING_OP_RENAMEAT,
 * IORING_OP_UNLINKAT,
 * IORING_OP_MKDIRAT,
 * IORING_OP_SYMLINKAT,
 * IORING_OP_LINKAT,
 * IORING_OP_MSG_RING,
 */
public class IOUringEventloop extends Eventloop implements IOUringCompletionQueueCallback {

    private final RingBuffer ringBuffer;
    private final FileDescriptor eventfd;
    public final IOUringSubmissionQueue sq;
    private final IOUringCompletionQueue cq;
    private final long eventfdReadBuf = PlatformDependent.allocateMemory(8);
    // we could use an array.
    final IntObjectMap<CompletionListener> completionListeners = new IntObjectHashMap<>(4096);
    final UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(true);
    protected final PooledByteBufAllocator iovArrayBufferAllocator = new PooledByteBufAllocator();

    public IOUringEventloop(int idx, String name, ILogger logger, Scheduler scheduler, boolean spin) {
        super(idx, name, logger, scheduler, spin);
        this.ringBuffer = Native.createRingBuffer(DEFAULT_RING_SIZE, DEFAULT_IOSEQ_ASYNC_THRESHOLD);
        this.sq = ringBuffer.ioUringSubmissionQueue();
        this.cq = ringBuffer.ioUringCompletionQueue();
        this.eventfd = Native.newBlockingEventFd();
        this.completionListeners.put(eventfd.intValue(), (fd, op, res, flags, data) -> sq_addEventRead());
    }

    @Override
    public void wakeup() {
        if (spin || Thread.currentThread() == this) {
            return;
        }

        if (wakeupNeeded.get() && wakeupNeeded.compareAndSet(true, false)) {
            // write to the evfd which will then wake-up epoll_wait(...)
            Native.eventFdWrite(eventfd.intValue(), 1L);
        }
    }

    public void accept(IOUringServerSocket serverChannel) throws IOException {
        execute(() -> {
            serverChannel.configure(this);
            serverChannel.accept();
        });
    }

    @Override
    protected void eventLoop() {
        sq_addEventRead();

        while (running) {
            runTasks();

            boolean moreWork = scheduler.tick();

            flushDirtySockets();

            if (cq.hasCompletions()) {
                cq.process(this);
            } else if (spin || moreWork) {
                sq.submit();
            } else {
                wakeupNeeded.set(true);
                if (concurrentRunQueue.isEmpty()) {
                    sq.submitAndWait();
                } else {
                    sq.submit();
                }
                wakeupNeeded.set(false);
            }
        }
    }

    private void sq_addEventRead() {
        sq.addEventFdRead(eventfd.intValue(), eventfdReadBuf, 0, 8, (short) 0);
    }

    public void sq_addRead(int fd, long bufferAddress, int pos, int limit, short extraData) {
        sq.addRead(fd, eventfdReadBuf, 0, 8, (short) 0);
    }

    @Override
    public void handle(int fd, int res, int flags, byte op, short data) {
        CompletionListener l = completionListeners.get(fd);
        if (l == null) {
            System.out.println("no listener found for fd:" + fd + " op:" + op);
        } else {
            l.handle(fd, res, flags, op, data);
        }
    }

    @Override
    public CompletableFuture<AsyncSocket> connect(AsyncSocket c, SocketAddress address) {
        IOUringAsyncSocket channel = (IOUringAsyncSocket) c;

        CompletableFuture<AsyncSocket> future = new CompletableFuture();

        try {
            System.out.println(getName() + " connectRequest to address:" + address);

            LinuxSocket socket = LinuxSocket.newSocketStream(false);
            socket.setBlocking();

            channel.configure(this, c.socketConfig, socket);

            if (socket.connect(address)) {
                logger.info(getName() + "Socket connected to " + address);
                execute(() -> {
                    completionListeners.put(socket.intValue(), channel);
                    channel.onConnectionEstablished();
                    future.complete(channel);
                });
            } else {
                future.completeExceptionally(new IOException("Could not connect to " + address));
            }
        } catch (Exception e) {
            e.printStackTrace();
            future.completeExceptionally(e);
        }

        return future;
    }
}

