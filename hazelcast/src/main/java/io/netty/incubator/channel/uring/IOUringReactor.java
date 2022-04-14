package io.netty.incubator.channel.uring;

import com.hazelcast.spi.impl.reactor.Channel;
import com.hazelcast.spi.impl.reactor.Reactor;
import com.hazelcast.spi.impl.reactor.SocketConfig;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.unix.FileDescriptor;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;
import io.netty.util.internal.PlatformDependent;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.netty.incubator.channel.uring.Native.DEFAULT_IOSEQ_ASYNC_THRESHOLD;
import static io.netty.incubator.channel.uring.Native.DEFAULT_RING_SIZE;
import static io.netty.incubator.channel.uring.Native.IORING_OP_ACCEPT;
import static io.netty.incubator.channel.uring.Native.IORING_OP_READ;
import static io.netty.incubator.channel.uring.Native.IORING_OP_WRITE;
import static io.netty.incubator.channel.uring.Native.IORING_OP_WRITEV;


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
public class IOUringReactor extends Reactor implements IOUringCompletionQueueCallback {

    private final boolean spin;
    private final RingBuffer ringBuffer;
    private final FileDescriptor eventfd;
    final IOUringSubmissionQueue sq;
    private final IOUringCompletionQueue cq;
    public final AtomicBoolean wakeupNeeded = new AtomicBoolean(true);
    private final long eventfdReadBuf = PlatformDependent.allocateMemory(8);
    private final IntObjectMap<IOUringServerChannel> serverChannels = new IntObjectHashMap<>(4096);
    // we could use an array.
    final IntObjectMap<IOUringChannel> channelMap = new IntObjectHashMap<>(4096);
    final UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(true);

    public IOUringReactor(IOUringReactorConfig config) {
        super(config);
        this.spin = config.spin;
        this.ringBuffer = Native.createRingBuffer(DEFAULT_RING_SIZE, DEFAULT_IOSEQ_ASYNC_THRESHOLD);
        this.sq = ringBuffer.ioUringSubmissionQueue();
        this.cq = ringBuffer.ioUringCompletionQueue();
        this.eventfd = Native.newBlockingEventFd();
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

    public void accept(IOUringServerChannel serverChannel) throws IOException {
        LinuxSocket serverSocket = LinuxSocket.newSocketStream(false);
        serverSocket.setBlocking();
        serverSocket.setReuseAddress(true);
        System.out.println(getName() + " serverSocket.fd:" + serverSocket.intValue());

        serverSocket.bind(serverChannel.address);
        System.out.println(getName() + " Bind success " + serverChannel.address);
        serverSocket.listen(10);
        System.out.println(getName() + " Listening on " + serverChannel.address);

        schedule(() -> {
            serverChannel.sq = sq;
            serverChannel.reactor = IOUringReactor.this;
            serverChannel.serverSocket = serverSocket;
            serverChannels.put(serverSocket.intValue(), serverChannel);
            serverChannel.sq_addAccept();
        });
    }

    @Override
    protected void eventLoop() {
        sq_addEventRead();

        while (running) {
            runTasks();

            boolean moreWork = scheduler.tick();

            flushDirtyChannels();

            if (!cq.hasCompletions()) {
                if (spin || moreWork) {
                    sq.submit();
                } else {
                    wakeupNeeded.set(true);
                    if (publicRunQueue.isEmpty()) {
                        sq.submitAndWait();
                    } else {
                        sq.submit();
                    }
                    wakeupNeeded.set(false);
                }
            } else {
                int processed = cq.process(this);
                if (processed > 0) {
                    //     System.out.println(getName() + " processed " + processed);
                }
            }
        }
    }

    private void sq_addEventRead() {
        sq.addEventFdRead(eventfd.intValue(), eventfdReadBuf, 0, 8, (short) 0);
    }


    @Override
    public void handle(int fd, int res, int flags, byte op, short data) {
        //System.out.println(getName() + " handle called: opcode:" + op);

        if (op == IORING_OP_READ) {
            // res is the number of bytes read
            //todo: we need to deal with res=0 and res<0
            if (fd == eventfd.intValue()) {
                //System.out.println(getName() + " handle IORING_OP_READ from eventFd res:" + res);
                sq_addEventRead();
            } else if (res < 0) {
                System.out.println("Problem: handle_IORING_OP_READ res:" + res);
            } else {
                // System.out.println(getName() + " handle IORING_OP_READ from fd:" + fd + " res:" + res + " flags:" + flags);
                IOUringChannel channel = channelMap.get(fd);
                channel.handle_IORING_OP_READ(res, flags, data);
            }
        } else if (op == IORING_OP_WRITE) {
            if (res < 0) {
                System.out.println("Problem: handle_IORING_OP_WRITEV res: " + res);
            } else {
                //System.out.println("handle_IORING_OP_WRITE fd:" + fd + " bytes written: " + res);
                IOUringChannel channel = channelMap.get(fd);
                channel.handle_IORING_OP_WRITE(res, flags, data);
            }
        } else if (op == IORING_OP_WRITEV) {
            if (res < 0) {
                System.out.println("Problem: handle_IORING_OP_WRITEV res: " + res);
            } else {
                //System.out.println("handle_IORING_OP_WRITEV fd:" + fd + " bytes written: " + res);
                IOUringChannel channel = channelMap.get(fd);
                channel.handle_IORING_OP_WRITEV(res, flags, data);
            }
        } else if (op == IORING_OP_ACCEPT) {
            if (res < 0) {
                System.out.println("Problem: IORING_OP_ACCEPT res: " + res);
            } else {
                IOUringServerChannel channel = serverChannels.get(fd);
                channel.handle_IORING_OP_ACCEPT(res, flags, data);
            }
        } else {
            System.out.println(this + " handle Unknown opcode:" + op);
        }
    }

    protected final PooledByteBufAllocator iovArrayBufferAllocator = new PooledByteBufAllocator();

    @Override
    public Future<Channel> connect(Channel c, SocketAddress address) {
        IOUringChannel channel = (IOUringChannel) c;

        CompletableFuture<Channel> future = new CompletableFuture();

        try {
            System.out.println(getName() + " connectRequest to address:" + address);

            LinuxSocket socket = LinuxSocket.newSocketStream(false);
            socket.setBlocking();

            channel.configure(IOUringReactor.this, c.socketConfig, socket);

            if (socket.connect(address)) {
                logger.info(getName() + "Socket connected to " + address);
                schedule(() -> {
                    channelMap.put(socket.intValue(), channel);
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

