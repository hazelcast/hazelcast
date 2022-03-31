package io.netty.incubator.channel.uring;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.spi.impl.reactor.Channel;
import com.hazelcast.spi.impl.reactor.ChannelConfig;
import com.hazelcast.spi.impl.reactor.Reactor;
import com.hazelcast.spi.impl.reactor.ReactorFrontEnd;
import com.hazelcast.spi.impl.reactor.Request;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.nio.IOUtil.compactOrClear;
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
    private final IntObjectMap<IO_UringChannel> channels = new IntObjectHashMap<>(4096);
    private final byte[] inet4AddressArray = new byte[SockaddrIn.IPV4_ADDRESS_LENGTH];

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
    public void schedule(Request request) {
        runQueue.add(request);
        wakeup();
    }

    public void schedule(IO_UringChannel channel) {
        runQueue.add(channel);
        wakeup();
    }

    @Override
    public Future<Channel> asyncConnect(SocketAddress address, Connection connection) {
        System.out.println("asyncConnect connect to " + address);

        ConnectRequest connectRequest = new ConnectRequest();
        connectRequest.address = address;
        connectRequest.connection = connection;
        connectRequest.future = new CompletableFuture<>();
        runQueue.add(connectRequest);

        wakeup();

        return connectRequest.future;
    }

    @Override
    public void setupServerSocket() throws Exception {
        this.serverSocket = LinuxSocket.newSocketStream(false);
        this.serverSocket.setBlocking();
        this.serverSocket.setReuseAddress(true);
        System.out.println(getName() + " serverSocket.fd:" + serverSocket.intValue());

        InetSocketAddress serverAddress = new InetSocketAddress(thisAddress.getInetAddress(), port);
        serverSocket.setTcpQuickAck(false);
        System.out.println(getName()+" serverSocket.rcv buffer size:"+serverSocket.getReceiveBufferSize());
        System.out.println(getName()+" serverSocket.snd buffer size:"+serverSocket.getSendBufferSize());
        serverSocket.setTcpNoDelay(true);
        serverSocket.bind(serverAddress);
        System.out.println(getName() + " Bind success " + serverAddress);
        serverSocket.listen(10);
        System.out.println(getName() + " Listening on " + serverAddress);
    }

    @NotNull
    private IO_UringChannel newChannel(LinuxSocket socket, Connection connection) {
        IO_UringChannel channel = new IO_UringChannel();
        channel.socket = socket;
        channel.localAddress = socket.localAddress();
        channel.reactor = this;
        UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(true);
        channel.readBuff = allocator.directBuffer(1024 * 64);
        channel.writeBuff = allocator.directBuffer(1024 * 64);
        channel.readBuffer = ByteBuffer.allocate(1024 * 64);
        channel.connection = connection;
        return channel;
    }

    @Override
    protected void eventLoop() {
        sq_addEventRead();
        sq_addAccept();

        // not needed
        sq.submit();

        while (!frontend.shuttingdown) {
            runTasks();

            // Check there were any completion events to process
            if (!cq.hasCompletions()) {
                if (spin) {
                    sq.submit();
                } else {
                    //System.out.println(getName() + " submitAndWait:started");
                    wakeupNeeded.set(true);
                    if (runQueue.isEmpty()) {
                        sq.submitAndWait();
                    } else {
                        sq.submit();
                    }
                    wakeupNeeded.set(false);
                }
                //System.out.println(getName() + " submitAndWait:completed");
            } else {
                int processed = cq.process(this);
                if (processed > 0) {
                    //     System.out.println(getName() + " processed " + processed);
                }
            }

            //sleep(500);
        }
    }

    private void sq_addEventRead() {
        sq.addEventFdRead(eventfd.intValue(), eventfdReadBuf, 0, 8, (short) 0);
    }

    private void sq_addAccept() {
        sq.addAccept(serverSocket.intValue(), acceptedAddressMemoryAddress, acceptedAddressLengthMemoryAddress, (short) 0);
    }

    private void sq_addRead(IO_UringChannel channel) {
        ByteBuf b = channel.readBuff;
        //System.out.println("sq_addRead writerIndex:" + b.writerIndex() + " capacity:" + b.capacity());
        sq.addRead(channel.socket.intValue(), b.memoryAddress(), b.writerIndex(), b.capacity(), (short) 0);
    }

    private void sq_addWrite(IO_UringChannel channel) {
        ByteBuf buf = channel.writeBuff;
        sq.addWrite(channel.socket.intValue(), buf.memoryAddress(), buf.readerIndex(), buf.writerIndex(), (short) 0);
    }

    @Override
    public void handle(int fd, int res, int flags, byte op, short data) {
        //System.out.println(getName() + " handle called: opcode:" + op);

        if (op == IORING_OP_READ) {
            handle_IORING_OP_READ(fd, res, flags, op, data);
        } else if (op == IORING_OP_WRITE) {
            handle_IORING_OP_WRITE(op, res, flags, op, data);
        } else if (op == IORING_OP_ACCEPT) {
            handle_IORING_OP_ACCEPT(op, res, flags, op, data);
        } else {
            System.out.println(this + " handle Unknown opcode:" + op);
        }
    }

    private void handle_IORING_OP_ACCEPT(int fd, int res, int flags, byte op, short data) {
        sq_addAccept();

        //System.out.println(getName() + " handle IORING_OP_ACCEPT fd:" + fd + " serverFd:" + serverSocket.intValue() + "res:" + res);

        if (res >= 0) {
            SocketAddress address = SockaddrIn.readIPv4(acceptedAddressMemoryAddress, inet4AddressArray);


            System.out.println(this + " new connected accepted: " + address);

            LinuxSocket socket = new LinuxSocket(res);
            try {
                socket.setTcpNoDelay(channelConfig.tcpNoDelay);
                socket.setTcpQuickAck(channelConfig.tcpQuickAck);
                socket.setSendBufferSize(channelConfig.sendBufferSize);
                socket.setReceiveBufferSize(channelConfig.receiveBufferSize);
            }catch (IOException e){
                throw new RuntimeException(e);
            }
            IO_UringChannel channel = newChannel(socket, null);
            channel.remoteAddress = address;
            channels.put(res, channel);
            sq_addRead(channel);
        }
    }

    private void handle_IORING_OP_WRITE(int fd, int res, int flags, byte op, short data) {
        //System.out.println(getName() + " handle called: opcode:" + op + " OP_WRITE");
    }

    private void handle_IORING_OP_READ(int fd, int res, int flags, byte op, short data) {
        // res is the number of bytes read

        if (fd == eventfd.intValue()) {
            //System.out.println(getName() + " handle IORING_OP_READ from eventFd res:" + res);
            sq_addEventRead();
            return;
        }

        //   System.out.println(getName() + " handle IORING_OP_READ from fd:" + fd + " res:" + res + " flags:" + flags);

        //System.out.println(getName()+" >>>>>>>>>>>>>>>>>>>>>>>>>>>>>> bytes read: "+res);

        IO_UringChannel channel = channels.get(fd);
        // we need to update the writerIndex; not done automatically.
        //todo: we need to deal with res=0 and res<0
        int oldLimit = channel.readBuffer.limit();
        channel.readBuffer.limit(res);
        channel.readBuff.writerIndex(channel.readBuff.writerIndex() + res);
        channel.readBuff.readBytes(channel.readBuffer);
        channel.readBuff.clear();
        channel.readBuffer.limit(oldLimit);
        sq_addRead(channel);

        channel.readBuffer.flip();
        for (; ; ) {
            Packet packet = channel.packetReader.readFrom(channel.readBuffer);
            //System.out.println(this + " ------------------------- read packet: " + packet);
            if (packet == null) {
                break;
            }

            channel.packetsRead++;
            packet.setConn((ServerConnection) channel.connection);
            packet.channel = channel;
            handlePacket(packet);
        }
        compactOrClear(channel.readBuffer);
    }

    private void runTasks() {
        for (; ; ) {
            Object item = runQueue.poll();
            if (item == null) {
                return;
            }

            if (item instanceof IO_UringChannel) {
                handleChannel((IO_UringChannel) item);
            } else if (item instanceof ConnectRequest) {
                handleConnectRequest((ConnectRequest) item);
//            } else if (item instanceof Op) {
//                handleOp((Op) item);
            } else if (item instanceof Request) {
                handleLocalRequest((Request) item);
            } else {
                throw new RuntimeException("Unrecognized type:" + item.getClass());
            }
        }
    }

    private void handleChannel(IO_UringChannel channel) {
        //System.out.println(getName() + " process channel " + channel.remoteAddress);

        // todo: if 'processChannel' gets called while a write is under way, we would be writing
        // to the same buffer, the kernel is currently reading from.

        ByteBuf buf = channel.writeBuff;
        buf.clear();
        //channel.bytesWritten = 0;
        for (; ; ) {
            ByteBuffer buffer = channel.next();
            if (buffer == null) {
                break;
            }
            channel.packetsWritten++;
            //packetsWritten++;
            //written += buffer.remaining();
            buf.writeBytes(buffer);
        }

        sq_addWrite(channel);

        channel.unschedule();
    }

    private void handleConnectRequest(ConnectRequest request) {
        try {
            SocketAddress address = request.address;
            System.out.println(getName() + " connectRequest to address:" + address);

            LinuxSocket socket = LinuxSocket.newSocketStream(false);
            socket.setBlocking();
            socket.setTcpNoDelay(channelConfig.tcpNoDelay);
            socket.setTcpQuickAck(channelConfig.tcpQuickAck);
            socket.setSendBufferSize(channelConfig.sendBufferSize);
            socket.setReceiveBufferSize(channelConfig.receiveBufferSize);

            if (!socket.connect(request.address)) {
                request.future.completeExceptionally(new IOException("Could not connect to " + request.address));
                return;
            }
            logger.info(getName() + "Socket connected to " + address);
            IO_UringChannel channel = newChannel(socket, request.connection);
            channel.remoteAddress = request.address;
            channels.put(socket.intValue(), channel);
            request.future.complete(channel);
            sq_addRead(channel);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
