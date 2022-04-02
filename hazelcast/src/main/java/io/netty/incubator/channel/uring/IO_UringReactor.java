package io.netty.incubator.channel.uring;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.spi.impl.reactor.ChannelConfig;
import com.hazelcast.spi.impl.reactor.Reactor;
import com.hazelcast.spi.impl.reactor.ReactorFrontEnd;
import com.hazelcast.spi.impl.reactor.Invocation;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.unix.Buffer;
import io.netty.channel.unix.FileDescriptor;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;
import io.netty.util.internal.PlatformDependent;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.nio.IOUtil.compactOrClear;
import static com.hazelcast.internal.nio.Packet.FLAG_OP_RESPONSE;
import static io.netty.incubator.channel.uring.Native.DEFAULT_IOSEQ_ASYNC_THRESHOLD;
import static io.netty.incubator.channel.uring.Native.DEFAULT_RING_SIZE;
import static io.netty.incubator.channel.uring.Native.IORING_OP_ACCEPT;
import static io.netty.incubator.channel.uring.Native.IORING_OP_READ;
import static io.netty.incubator.channel.uring.Native.IORING_OP_WRITE;


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
public class IO_UringReactor extends Reactor implements IOUringCompletionQueueCallback {

//    static {
//        System.out.println("IORING_OP_POLL_ADD:" + IORING_OP_POLL_ADD);
//        System.out.println("IORING_OP_TIMEOUT:" + IORING_OP_TIMEOUT);
//        System.out.println("IORING_OP_ACCEPT:" + IORING_OP_ACCEPT);
//        System.out.println("IORING_OP_READ:" + IORING_OP_READ);
//        System.out.println("IORING_OP_WRITE:" + IORING_OP_WRITE);
//        System.out.println("IORING_OP_POLL_REMOVE:" + IORING_OP_POLL_REMOVE);
//        System.out.println("IORING_OP_CONNECT:" + IORING_OP_CONNECT);
//        System.out.println("IORING_OP_CLOSE:" + IORING_OP_CLOSE);
//        System.out.println("IORING_OP_WRITEV:" + IORING_OP_WRITEV);
//        System.out.println("IORING_OP_SENDMSG:" + IORING_OP_SENDMSG);
//        System.out.println("IORING_OP_RECVMSG:" + IORING_OP_RECVMSG);
//    }

    private final boolean spin;
    public final ConcurrentLinkedQueue runQueue = new ConcurrentLinkedQueue();
    private final RingBuffer ringBuffer;
    private final FileDescriptor eventfd;
    private LinuxSocket serverSocket;
    private final IOUringSubmissionQueue sq;
    private final IOUringCompletionQueue cq;
    public final AtomicBoolean wakeupNeeded = new AtomicBoolean(true);
    private ByteBuffer acceptedAddressMemory;
    private long acceptedAddressMemoryAddress;
    private ByteBuffer acceptedAddressLengthMemory;
    private long acceptedAddressLengthMemoryAddress;
    private final long eventfdReadBuf = PlatformDependent.allocateMemory(8);

    // we could use an array.
    private final IntObjectMap<IO_UringChannel> channelMap = new IntObjectHashMap<>(4096);
    private final byte[] inet4AddressArray = new byte[SockaddrIn.IPV4_ADDRESS_LENGTH];
    private final UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(true);

    public IO_UringReactor(ReactorFrontEnd frontend, ChannelConfig channelConfig, Address thisAddress, int port, boolean spin) {
        super(frontend, channelConfig, thisAddress, port, "IOUringReactor:[" + thisAddress.getHost() + ":" + thisAddress.getPort() + "]:" + port);
        this.spin = spin;
        this.ringBuffer = Native.createRingBuffer(DEFAULT_RING_SIZE, DEFAULT_IOSEQ_ASYNC_THRESHOLD);
        this.sq = ringBuffer.ioUringSubmissionQueue();
        this.cq = ringBuffer.ioUringCompletionQueue();
        this.eventfd = Native.newBlockingEventFd();
        //this.recvByteAllocatorHandle = new IOUringRecvByteAllocatorHandle((RecvByteBufAllocator.ExtendedHandle) new ServerChannelRecvByteBufAllocator().newHandle());

        this.acceptedAddressMemory = Buffer.allocateDirectWithNativeOrder(Native.SIZEOF_SOCKADDR_STORAGE);
        this.acceptedAddressMemoryAddress = Buffer.memoryAddress(acceptedAddressMemory);
        this.acceptedAddressLengthMemory = Buffer.allocateDirectWithNativeOrder(Long.BYTES);
        // Needs to be initialized to the size of acceptedAddressMemory.
        // See https://man7.org/linux/man-pages/man2/accept.2.html
        this.acceptedAddressLengthMemory.putLong(0, Native.SIZEOF_SOCKADDR_STORAGE);
        this.acceptedAddressLengthMemoryAddress = Buffer.memoryAddress(acceptedAddressLengthMemory);
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

    @Override
    public void schedule(Invocation request) {
        runQueue.add(request);
        wakeup();
    }

    public void schedule(IO_UringChannel channel) {
        if (runQueue.contains(channel)) {
            throw new RuntimeException("duplicate");
        }

        runQueue.add(channel);
        wakeup();
    }

    @Override
    public void schedule(ConnectRequest request) {
        runQueue.add(request);
        wakeup();
    }

    @Override
    public void setupServerSocket() throws Exception {
        this.serverSocket = LinuxSocket.newSocketStream(false);
        this.serverSocket.setBlocking();
        this.serverSocket.setReuseAddress(true);
        System.out.println(getName() + " serverSocket.fd:" + serverSocket.intValue());

        InetSocketAddress serverAddress = new InetSocketAddress(thisAddress.getInetAddress(), port);
        serverSocket.bind(serverAddress);
        System.out.println(getName() + " Bind success " + serverAddress);
        serverSocket.listen(10);
        System.out.println(getName() + " Listening on " + serverAddress);
    }

    private void configure(LinuxSocket socket) throws IOException {
        socket.setTcpNoDelay(channelConfig.tcpNoDelay);
        socket.setSendBufferSize(channelConfig.sendBufferSize);
        socket.setReceiveBufferSize(channelConfig.receiveBufferSize);
        socket.setTcpQuickAck(channelConfig.tcpQuickAck);

        String id = socket.localAddress() + "->" + socket.remoteAddress();
        System.out.println(getName() + " " + id + " tcpNoDelay: " + socket.isTcpNoDelay());
        System.out.println(getName() + " " + id + " tcpQuickAck: " + socket.isTcpQuickAck());
        System.out.println(getName() + " " + id + " receiveBufferSize: " + socket.getReceiveBufferSize());
        System.out.println(getName() + " " + id + " sendBufferSize: " + socket.getSendBufferSize());
    }

    @NotNull
    private IO_UringChannel newChannel(LinuxSocket socket, Connection connection) {
        IO_UringChannel channel = new IO_UringChannel();
        channel.socket = socket;
        channel.localAddress = socket.localAddress();
        channel.remoteAddress = socket.remoteAddress();
        channel.reactor = this;
        channel.receiveBuff = allocator.directBuffer(channelConfig.receiveBufferSize);
        channel.writeBufs = new ByteBuf[1024];
        channel.writeBufsInUse = new boolean[channel.writeBufs.length];
        for (int k = 0; k < channel.writeBufs.length; k++) {
            channel.writeBufs[k] = allocator.directBuffer(8192);
        }
        channel.readBuffer = ByteBuffer.allocate(channelConfig.sendBufferSize);
        channel.connection = connection;
        channels.add(channel);
        return channel;
    }

    @Override
    protected void eventLoop() {
        sq_addEventRead();
        sq_addAccept();

        while (!frontend.shuttingdown) {
            runTasks();

            if (!cq.hasCompletions()) {
                if (spin) {
                    sq.submit();
                } else {
                    wakeupNeeded.set(true);
                    if (runQueue.isEmpty()) {
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

    private void sq_addAccept() {
        sq.addAccept(serverSocket.intValue(), acceptedAddressMemoryAddress, acceptedAddressLengthMemoryAddress, (short) 0);
    }

    private void sq_addRead(IO_UringChannel channel) {
        ByteBuf b = channel.receiveBuff;
        //System.out.println("sq_addRead writerIndex:" + b.writerIndex() + " capacity:" + b.capacity());
        sq.addRead(channel.socket.intValue(), b.memoryAddress(), b.writerIndex(), b.capacity(), (short) 0);
    }

    private void sq_addWrite(IO_UringChannel channel, ByteBuf buf, short index) {
        sq.addWrite(channel.socket.intValue(), buf.memoryAddress(), buf.readerIndex(), buf.writerIndex(), index);
    }

    @Override
    public void handle(int fd, int res, int flags, byte op, short data) {
        //System.out.println(getName() + " handle called: opcode:" + op);

        if (op == IORING_OP_READ) {
            handle_IORING_OP_READ(fd, res, flags, data);
        } else if (op == IORING_OP_WRITE) {
            handle_IORING_OP_WRITE(fd, res, flags, data);
        } else if (op == IORING_OP_ACCEPT) {
            handle_IORING_OP_ACCEPT(fd, res, flags, data);
        } else {
            System.out.println(this + " handle Unknown opcode:" + op);
        }
    }

    private void handle_IORING_OP_ACCEPT(int fd, int res, int flags, short data) {
        sq_addAccept();

        System.out.println(getName() + " handle IORING_OP_ACCEPT fd:" + fd + " serverFd:" + serverSocket.intValue() + "res:" + res);

        if (res >= 0) {
            SocketAddress address = SockaddrIn.readIPv4(acceptedAddressMemoryAddress, inet4AddressArray);

            System.out.println(this + " new connected accepted: " + address);

            LinuxSocket socket = new LinuxSocket(res);
            try {
                configure(socket);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            IO_UringChannel channel = newChannel(socket, null);
            channel.remoteAddress = address;
            channelMap.put(res, channel);
            sq_addRead(channel);
        }
    }

    private long handle_IORING_OP_WRITE = 0;

    private void handle_IORING_OP_WRITE(int fd, int res, int flags, short data) {
        //todo: deal with negative res.

        // data is the index in the channel.writeBufsInUse array.

        //System.out.println("handle_IORING_OP_WRITE fd:"+fd);
        //System.out.println("res:"+res);
        IO_UringChannel channel = channelMap.get(fd);
        channel.bytesWrittenConfirmed += res;

        ByteBuf buf = channel.writeBufs[data];
        if (buf.readableBytes() != res) {
            throw new RuntimeException("Readable bytes doesn't match res. Buffer:" + buf + " res:" + res);
        }
        channel.writeBufsInUse[data] = false;


        //System.out.println(getName() + " handle called: opcode:" + op + " OP_WRITE");
    }

    private void handle_IORING_OP_READ(int fd, int res, int flags, short data) {
        // res is the number of bytes read

        if (fd == eventfd.intValue()) {
            //System.out.println(getName() + " handle IORING_OP_READ from eventFd res:" + res);
            sq_addEventRead();
            return;
        }

        //   System.out.println(getName() + " handle IORING_OP_READ from fd:" + fd + " res:" + res + " flags:" + flags);

        //System.out.println(getName()+" >>>>>>>>>>>>>>>>>>>>>>>>>>>>>> bytes read: "+res);

        IO_UringChannel channel = channelMap.get(fd);
        // we need to update the writerIndex; not done automatically.
        //todo: we need to deal with res=0 and res<0
        int oldLimit = channel.readBuffer.limit();
        channel.readEvents += 1;
        channel.bytesRead += res;
        channel.readBuffer.limit(res);
        channel.receiveBuff.writerIndex(channel.receiveBuff.writerIndex() + res);
        channel.receiveBuff.readBytes(channel.readBuffer);
        channel.receiveBuff.clear();
        channel.readBuffer.limit(oldLimit);
        sq_addRead(channel);

        Packet responseChain = null;
        channel.readBuffer.flip();
        for (; ; ) {
            Packet packet = channel.packetReader.readFrom(channel.readBuffer);
            if (packet == null) {
                break;
            }

            channel.packetsRead++;
            packet.setConn((ServerConnection) channel.connection);
            packet.channel = channel;

            if (packet.isFlagRaised(FLAG_OP_RESPONSE)) {
                packet.next = responseChain;
                responseChain = packet;
                //frontend.handleResponse(packet);
            } else {
                handleRemoteOp(packet);
            }
        }

        if (responseChain != null) {
            frontend.handleResponse(responseChain);
        }

        //channel.flush();not needed
        compactOrClear(channel.readBuffer);

        //todo: respnses get unwanted handleChannel
        if (!channel.scheduled.get() && channel.scheduled.compareAndSet(false, true)) {
            // if it is already scheduled, we don't need to process outbound since
            // it is guaranteed to be done at some point in the future.
            handleOutbound(channel);
        }
    }

    private void runTasks() {
        for (; ; ) {
            Object task = runQueue.poll();
            if (task == null) {
                return;
            }

            if (task instanceof IO_UringChannel) {
                handleOutbound((IO_UringChannel) task);
            } else if (task instanceof ConnectRequest) {
                handleConnectRequest((ConnectRequest) task);
            } else if (task instanceof Invocation) {
                handleLocalOp((Invocation) task);
            } else {
                throw new RuntimeException("Unrecognized type:" + task.getClass());
            }
        }
    }

    //   private int handleOutbound = 0;

    private void handleOutbound(IO_UringChannel channel) {
        channel.handleOutboundCalls++;
        //System.out.println(getName() + " process channel " + channel.remoteAddress);

        short bufIndex = -1;
        for (short k = 0; k < channel.writeBufsInUse.length; k++) {
            if (!channel.writeBufsInUse[k]) {
                bufIndex = k;
                break;
            }
        }

        //System.out.println("index:"+index);
        channel.writeBufsInUse[bufIndex] = true;
        ByteBuf buf = channel.writeBufs[bufIndex];
        buf.clear();

        int bytesWritten = 0;

        for (; ; ) {
            ByteBuffer buffer = channel.current;

            if (buffer == null) {
                buffer = channel.pending.poll();
            }

            if (buffer == null) {
                break;
            }

            channel.packetsWritten++;
            channel.bytesWritten += buffer.remaining();
            bytesWritten += buffer.remaining();
            buf.writeBytes(buffer);

            if (buffer.hasRemaining()) {
                channel.current = buffer;
            } else {
                channel.current = null;
            }
        }

        if (buf.readableBytes() != bytesWritten) {
            throw new RuntimeException("Data lost: " + buf + " bytes written:" + bytesWritten);
        }

        sq_addWrite(channel, buf, bufIndex);

        channel.unschedule();
    }

    private void handleConnectRequest(ConnectRequest request) {
        try {
            SocketAddress address = request.address;
            System.out.println(getName() + " connectRequest to address:" + address);

            LinuxSocket socket = LinuxSocket.newSocketStream(false);
            socket.setBlocking();
            configure(socket);

            if (!socket.connect(request.address)) {
                request.future.completeExceptionally(new IOException("Could not connect to " + request.address));
                return;
            }
            logger.info(getName() + "Socket connected to " + address);
            IO_UringChannel channel = newChannel(socket, request.connection);
            channel.remoteAddress = request.address;
            channelMap.put(socket.intValue(), channel);
            request.future.complete(channel);
            sq_addRead(channel);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
