package io.netty.incubator.channel.uring;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.serialization.impl.ByteArrayObjectDataInput;
import com.hazelcast.internal.serialization.impl.ByteArrayObjectDataOutput;
import com.hazelcast.internal.util.ThreadAffinity;
import com.hazelcast.internal.util.ThreadAffinityHelper;
import com.hazelcast.internal.util.executor.HazelcastManagedThread;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.reactor.Op;
import com.hazelcast.spi.impl.reactor.Request;
import com.hazelcast.spi.impl.reactor.nio.NioChannel;
import com.hazelcast.table.impl.SelectByKeyOperation;
import com.hazelcast.table.impl.UpsertOperation;
import io.netty.channel.unix.Buffer;
import io.netty.channel.unix.FileDescriptor;
import io.netty.util.NetUtil;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;
import io.netty.util.internal.PlatformDependent;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.BitSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.spi.impl.reactor.Op.RUN_CODE_DONE;
import static com.hazelcast.spi.impl.reactor.Op.RUN_CODE_FOO;
import static com.hazelcast.spi.impl.reactor.OpCodes.TABLE_SELECT_BY_KEY;
import static com.hazelcast.spi.impl.reactor.OpCodes.TABLE_UPSERT;
import static io.netty.incubator.channel.uring.Native.DEFAULT_IOSEQ_ASYNC_THRESHOLD;
import static io.netty.incubator.channel.uring.Native.DEFAULT_RING_SIZE;
import static io.netty.incubator.channel.uring.Native.IORING_OP_ACCEPT;
import static io.netty.incubator.channel.uring.Native.IORING_OP_CLOSE;
import static io.netty.incubator.channel.uring.Native.IORING_OP_CONNECT;
import static io.netty.incubator.channel.uring.Native.IORING_OP_POLL_ADD;
import static io.netty.incubator.channel.uring.Native.IORING_OP_POLL_REMOVE;
import static io.netty.incubator.channel.uring.Native.IORING_OP_READ;
import static io.netty.incubator.channel.uring.Native.IORING_OP_RECVMSG;
import static io.netty.incubator.channel.uring.Native.IORING_OP_SENDMSG;
import static io.netty.incubator.channel.uring.Native.IORING_OP_TIMEOUT;
import static io.netty.incubator.channel.uring.Native.IORING_OP_WRITE;
import static io.netty.incubator.channel.uring.Native.IORING_OP_WRITEV;


/**
 * To build io uring:
 *
 * sudo yum install autoconf
 * sudo yum install automake
 * sudo yum install libtool
 *
 * Good read:
 * https://unixism.net/2020/04/io-uring-by-example-part-3-a-web-server-with-io-uring/
 *
 *
 * no syscalls:
 * https://wjwh.eu/posts/2021-10-01-no-syscall-server-iouring.html
 *
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
public class IO_UringReactor extends Thread implements IOUringCompletionQueueCallback {

    static {
        System.out.println("IORING_OP_POLL_ADD:" + IORING_OP_POLL_ADD);
        System.out.println("IORING_OP_TIMEOUT:" + IORING_OP_TIMEOUT);
        System.out.println("IORING_OP_ACCEPT:" + IORING_OP_ACCEPT);
        System.out.println("IORING_OP_READ:" + IORING_OP_READ);
        System.out.println("IORING_OP_WRITE:" + IORING_OP_WRITE);
        System.out.println("IORING_OP_POLL_REMOVE:" + IORING_OP_POLL_REMOVE);
        System.out.println("IORING_OP_CONNECT:" + IORING_OP_CONNECT);
        System.out.println("IORING_OP_CLOSE:" + IORING_OP_CLOSE);
        System.out.println("IORING_OP_WRITEV:" + IORING_OP_WRITEV);
        System.out.println("IORING_OP_SENDMSG:" + IORING_OP_SENDMSG);
        System.out.println("IORING_OP_RECVMSG:" + IORING_OP_RECVMSG);
    }

    private final IO_UringReactorFrontEnd frontend;
    //private final Selector selector;
    private final ILogger logger;
    private final int port;
    private final Address thisAddress;
    private final boolean spin;
    //    private ServerSocketChannel serverSocketChannel;
    public final ConcurrentLinkedQueue taskQueue = new ConcurrentLinkedQueue();
    private final RingBuffer ringBuffer;
    private final FileDescriptor eventfd;
    private LinuxSocket serverSocket;
    private final IOUringSubmissionQueue sq;
    private final IOUringCompletionQueue cq;
    private BitSet allowedCpus;
    private final AtomicBoolean wakeupNeeded = new AtomicBoolean();
    private long nextPrint = System.currentTimeMillis() + 1000;
    private ByteBuffer acceptedAddressMemory;
    private long acceptedAddressMemoryAddress;
    private ByteBuffer acceptedAddressLengthMemory;
    private long acceptedAddressLengthMemoryAddress;
    private final long eventfdReadBuf = PlatformDependent.allocateMemory(8);
    private final IntObjectMap<IO_UringChannel> channels = new IntObjectHashMap<>(4096);
    private final byte[] inet4AddressArray = new byte[SockaddrIn.IPV4_ADDRESS_LENGTH];

    public IO_UringReactor(IO_UringReactorFrontEnd frontend, Address thisAddress, int port, boolean spin) {
        super("IOUringReactor:[" + thisAddress.getHost() + ":" + thisAddress.getPort() + "]:" + port);
        this.spin = spin;
        this.thisAddress = thisAddress;
        this.frontend = frontend;
        this.logger = frontend.logger;
        //this.selector = SelectorOptimizer.newSelector(frontend.logger);
        this.port = port;
        this.ringBuffer = Native.createRingBuffer(DEFAULT_RING_SIZE, DEFAULT_IOSEQ_ASYNC_THRESHOLD);
        this.sq = ringBuffer.ioUringSubmissionQueue();
        this.cq = ringBuffer.ioUringCompletionQueue();
        this.eventfd = Native.newBlockingEventFd();

        this.acceptedAddressMemory = Buffer.allocateDirectWithNativeOrder(Native.SIZEOF_SOCKADDR_STORAGE);
        this.acceptedAddressMemoryAddress = Buffer.memoryAddress(acceptedAddressMemory);
        this.acceptedAddressLengthMemory = Buffer.allocateDirectWithNativeOrder(Long.BYTES);
        // Needs to be initialized to the size of acceptedAddressMemory.
        // See https://man7.org/linux/man-pages/man2/accept.2.html
        this.acceptedAddressLengthMemory.putLong(0, Native.SIZEOF_SOCKADDR_STORAGE);
        this.acceptedAddressLengthMemoryAddress = Buffer.memoryAddress(acceptedAddressLengthMemory);
    }

    public void setThreadAffinity(ThreadAffinity threadAffinity) {
        this.allowedCpus = threadAffinity.nextAllowedCpus();
    }

    public void wakeup() {
        if (spin || Thread.currentThread() == this) {
            return;
        }

        if (wakeupNeeded.get() && wakeupNeeded.compareAndSet(true, false)) {
            // write to the evfd which will then wake-up epoll_wait(...)
            Native.eventFdWrite(eventfd.intValue(), 1L);
        }
    }

    public void enqueue(Request request) {
        taskQueue.add(request);
        wakeup();
    }

    public Future<IO_UringChannel> enqueue(SocketAddress address, Connection connection) {
        logger.info("Connect to " + address);

        ConnectRequest connectRequest = new ConnectRequest();
        connectRequest.address = address;
        connectRequest.connection = connection;
        connectRequest.future = new CompletableFuture<>();
        taskQueue.add(connectRequest);

        wakeup();

        return connectRequest.future;
    }

    static class ConnectRequest {
        Connection connection;
        SocketAddress address;
        CompletableFuture<IO_UringChannel> future;
    }

    @Override
    public void run() {
        setThreadAffinity();

        try {
            if (setupListeningSocket()) {
                eventLoop();
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }

        System.out.println(getName() + " Died: frontend shutting down:" + frontend.shuttingdown);
    }

    private void setThreadAffinity() {
        if (allowedCpus == null) {
            return;
        }

        ThreadAffinityHelper.setAffinity(allowedCpus);
        BitSet actualCpus = ThreadAffinityHelper.getAffinity();
        ILogger logger = Logger.getLogger(HazelcastManagedThread.class);
        if (!actualCpus.equals(allowedCpus)) {
            logger.warning(getName() + " affinity was not applied successfully. "
                    + "Expected CPUs:" + allowedCpus + ". Actual CPUs:" + actualCpus);
        } else {
            logger.info(getName() + " has affinity for CPUs:" + allowedCpus);
        }
    }

    private boolean setupListeningSocket() {
        InetSocketAddress bindAddress = null;
        try {
            this.serverSocket = LinuxSocket.newSocketStream(false);
            this.serverSocket.setReuseAddress(true);

            System.out.println(getName() + " serverSocket.fd:" + serverSocket.intValue());

            bindAddress = new InetSocketAddress(thisAddress.getInetAddress(), port);
            //serverSocket.setTcpQuickAck(false);
            //serverSocket.setTcpNoDelay(true);
            serverSocket.bind(bindAddress);
            System.out.println(getName() + " Bind success " + bindAddress);
            serverSocket.listen(NetUtil.SOMAXCONN);
            System.out.println(getName() + " Listening on " + bindAddress);
            return true;
        } catch (IOException e) {
            logger.severe(getName() + " Could not bind to " + bindAddress);
        }
        return false;
    }

    private void eventLoop() throws Exception {
        sq.addEventFdRead(eventfd.intValue(), eventfdReadBuf, 0, 8, (short) 0);
        sq.addAccept(serverSocket.intValue(), acceptedAddressMemoryAddress, acceptedAddressLengthMemoryAddress, (short) 0);

        // not needed
        sq.submit();

        while (!frontend.shuttingdown) {
            // Check there were any completion events to process
            if (!cq.hasCompletions()) {
                //System.out.println(getName() + " submitAndWait:started");
                wakeupNeeded.set(true);
                sq.submitAndWait();
                sq.submit();
                wakeupNeeded.set(false);
                //System.out.println(getName() + " submitAndWait:completed");
            } else {
                int processed = cq.process(this);
                if (processed > 0) {
                    System.out.println(getName() + " processed " + processed);
                }
            }

            Thread.sleep(500);
            processTasks();
        }
    }

    @Override
    public void handle(int fd, int res, int flags, byte op, short data) {
        System.out.println(getName() + " handle called: opcode:" + op);

        if (op == IORING_OP_READ && eventfd.intValue() == fd) {
            System.out.println(getName() + " handle Native.IORING_OP_READ");

            //pendingWakeup = false;
            sq.addEventFdRead(eventfd.intValue(), eventfdReadBuf, 0, 8, (short) 0);
        } else if (op == IORING_OP_TIMEOUT) {
            System.out.println(getName() + " handle Native.IORING_OP_TIMEOUT");
            //if (res == Native.ERRNO_ETIME_NEGATIVE) {
            //    prevDeadlineNanos = NONE;
            // }
        } else {
            // Remaining events should be channel-specific
            IO_UringChannel channel = channels.get(fd);

            if (op == IORING_OP_ACCEPT) {
                System.out.println(getName() + " handle Native.IORING_OP_ACCEPT fd:" + fd + " serverFd:" + serverSocket.intValue()+ "res:"+res);

                SocketAddress address = SockaddrIn.readIPv4(acceptedAddressMemoryAddress, inet4AddressArray);
                System.out.println(address);
                if (res >= 0) {
                    // the event source is the server socket
                    // But how can we determine the fd for the new channel
                    LinuxSocket socket = new LinuxSocket(res);

//                   return new IOUringSocketChannel(this, new LinuxSocket(fd), address);
//
                    channel = new IO_UringChannel();
                    channel.socket = socket;
                    channel.reactor = this;

                    channels.put(fd, channel);

                    //sq.addEventFdRead(res, )
                    // register for read.

                }

                sq.addAccept(serverSocket.intValue(), acceptedAddressMemoryAddress, acceptedAddressLengthMemoryAddress, (short) 0);
                return;
            }

            if (channel == null) {
                System.out.println("no channel found");
                return;
            }

            if (op == IORING_OP_READ || op == IORING_OP_RECVMSG) {

                // handleRead(channel, res, data);
            } else if (op == IORING_OP_WRITEV || op == IORING_OP_WRITE || op == IORING_OP_SENDMSG) {
                //handleWrite(channel, res, data);
            } else if (op == IORING_OP_POLL_ADD) {
                //handlePollAdd(channel, res, data);
            } else if (op == IORING_OP_POLL_REMOVE) {
//                if (res == Errors.ERRNO_ENOENT_NEGATIVE) {
//                    logger.trace("IORING_POLL_REMOVE not successful");
//                } else if (res == 0) {
//                    logger.trace("IORING_POLL_REMOVE successful");
//                }
//                channel.clearPollFlag(data);
//                if (!channel.ioScheduled()) {
//                    // We cancelled the POLL ops which means we are done and should remove the mapping.
//                    remove(channel);
//                    return;
//                }
            } else if (op == IORING_OP_CONNECT) {
                //handleConnect(channel, res);
            }
            // channel.ioUringUnsafe().processDelayedClose();
        }

        //todo: once we determine that the request was an accept, we need to call 'addAcceptRequest'
    }


//    private void processSelectionKeys() throws IOException {
//        Set<SelectionKey> selectionKeys = selector.selectedKeys();
//        Iterator<SelectionKey> it = selectionKeys.iterator();
//        while (it.hasNext()) {
//            SelectionKey key = it.next();
//            it.remove();
//
//            if (key.isValid() && key.isAcceptable()) {
//                SocketChannel socketChannel = serverSocketChannel.accept();
//                socketChannel.configureBlocking(false);
//                SelectionKey selectionKey = socketChannel.register(selector, OP_READ);
//                selectionKey.attach(newChannel(socketChannel, null));
//                 we
//                logger.info("Connection Accepted: " + socketChannel.getLocalAddress());
//            }
//
//            if (key.isValid() && key.isReadable()) {
//                SocketChannel socketChannel = (SocketChannel) key.channel();
//                Channel channel = (Channel) key.attachment();
//                ByteBuffer readBuf = channel.readBuff;
//                int bytesRead = socketChannel.read(readBuf);
//                //System.out.println(this + " bytes read: " + bytesRead);
//                if (bytesRead == -1) {
//                    socketChannel.close();
//                    key.cancel();
//                } else {
//                    channel.bytesRead += bytesRead;
//                    readBuf.flip();
//                    process(readBuf, channel);
//                    compactOrClear(readBuf);
//                }
//            }
//
//            if (!key.isValid()) {
//                //System.out.println("sk not valid");
//                key.cancel();
//            }
//        }
//    }

//    private Channel newChannel(SocketChannel socketChannel, Connection connection) {
//        System.out.println(this + " newChannel: " + socketChannel);
//
//        Channel channel = new Channel();
//        channel.reactor = this;
//        channel.readBuff = ByteBuffer.allocate(256 * 1024);
//        channel.socketChannel = socketChannel;
//        channel.connection = connection;
//        return channel;
//    }

    private void processTasks() {
        for (; ; ) {
            Object item = taskQueue.poll();
            if (item == null) {
                return;
            }

            if (item instanceof NioChannel) {
                process((NioChannel) item);
            } else if (item instanceof ConnectRequest) {
                process((ConnectRequest) item);
            } else if (item instanceof Op) {
                proces((Op) item);
            } else if (item instanceof Request) {
                proces((Request) item);
            } else {
                throw new RuntimeException("Unregonized type:" + item.getClass());
            }
        }
    }

    private void process(NioChannel channel) {
        //System.out.println("Processing channel");
        try {
            for (; ; ) {
                ByteBuffer buffer = channel.pending.poll();
                if (buffer == null) {
                    break;
                }

                channel.writeBuffs[channel.writeBuffLen] = buffer;
                channel.writeBuffLen++;
            }

            long written = channel.socketChannel.write(channel.writeBuffs, 0, channel.writeBuffLen);
            channel.bytesWritten += written;

            int toIndex = 0;
            int length = channel.writeBuffLen;
            for (int pos = 0; pos < length; pos++) {
                if (channel.writeBuffs[pos].hasRemaining()) {
                    if (pos == 0) {
                        // the first one is not empty, we are done
                        break;
                    } else {
                        channel.writeBuffs[toIndex] = channel.writeBuffs[pos];
                        channel.writeBuffs[pos] = null;
                        toIndex++;
                    }
                } else {
                    channel.writeBuffLen--;
                    channel.writeBuffs[pos] = null;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void process(ConnectRequest connectRequest) {
        try {
            SocketAddress address = connectRequest.address;
            System.out.println(getName() + " connectRequest to address:" + address);

            LinuxSocket socket = LinuxSocket.newSocketStream(false);
            //socket.setTcpNoDelay(true);
            //socket.setTcpQuickAck(false);

            if (!socket.connect(connectRequest.address)) {
                connectRequest.future.completeExceptionally(new IOException("Could not connect to " + connectRequest.address));
                return;
            }

            IO_UringChannel channel = new IO_UringChannel();
            channel.reactor = this;
            channel.socket = socket;

            channels.put(socket.intValue(), channel);

            logger.info(getName() + "Socket connected to " + address);
            connectRequest.future.complete(channel);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

//    private void process(ByteBuffer buffer, Channel channel) {
//        for (; ; ) {
//            Packet packet = packetIOHelper.readFrom(buffer);
//            //System.out.println(this + " read packet: " + packet);
//            if (packet == null) {
//                return;
//            }
//
//            channel.packetsRead++;
//
//            packet.setConn((ServerConnection) channel.connection);
//            packet.channel = channel;
//            process(packet);
//        }
//    }

    private void process(Packet packet) {
        //System.out.println(this + " process packet: " + packet);
        try {
            if (packet.isFlagRaised(Packet.FLAG_OP_RESPONSE)) {
                //System.out.println("Received remote response: "+packet);
                frontend.handleResponse(packet);
            } else {
                byte[] bytes = packet.toByteArray();
                byte opcode = bytes[Packet.DATA_OFFSET];
                Op op = allocateOp(opcode);
                op.in.init(packet.toByteArray(), Packet.DATA_OFFSET + 1);
                proces(op);

                //System.out.println("We need to send response to "+op.callId);
                ByteArrayObjectDataOutput out = op.out;
                ByteBuffer byteBuffer = ByteBuffer.wrap(out.toByteArray(), 0, out.position());
                packet.channel.writeAndFlush(byteBuffer);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // local call
    private void proces(Request request) {

        //System.out.println("request: " + request);
        try {
            byte[] data = request.out.toByteArray();
            byte opcode = data[0];
            Op op = allocateOp(opcode);
            op.in.init(data, 1);
            proces(op);
            request.invocation.completableFuture.complete(null);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void proces(Op op) {
        try {
            long callId = op.in.readLong();
            op.callId = callId;
            //System.out.println("callId: "+callId);
            int runCode = op.run();
            switch (runCode) {
                case RUN_CODE_DONE:
                    free(op);
                    return;
                case RUN_CODE_FOO:
                    throw new RuntimeException();
                default:
                    throw new RuntimeException();
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // use pool
    private Op allocateOp(int opcode) {
        Op op;
        switch (opcode) {
            case TABLE_UPSERT:
                op = new UpsertOperation();
                break;
            case TABLE_SELECT_BY_KEY:
                op = new SelectByKeyOperation();
                break;
            default://hack
                op = new UpsertOperation();
                //throw new RuntimeException("Unrecognized opcode:" + opcode);
        }
        op.in = new ByteArrayObjectDataInput(null, frontend.ss, ByteOrder.BIG_ENDIAN);
        op.out = new ByteArrayObjectDataOutput(64, frontend.ss, ByteOrder.BIG_ENDIAN);
        op.managers = frontend.managers;
        return op;
    }

    private void free(Op op) {
        op.cleanup();

        //we should return it to the pool.
    }
}
