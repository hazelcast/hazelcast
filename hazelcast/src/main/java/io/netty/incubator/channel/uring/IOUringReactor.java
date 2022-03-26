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


/**
 * To build io uring:
 *
 * sudo yum install autoconf
 * sudo yum install automake
 * sudo yum install libtool
 *
 * Good read:
 * https://unixism.net/2020/04/io-uring-by-example-part-3-a-web-server-with-io-uring/
 */
public class IOUringReactor extends Thread implements IOUringCompletionQueueCallback {
    private final IOUringReactorFrontEnd frontend;
    //private final Selector selector;
    private final ILogger logger;
    private final int port;
    private final Address thisAddress;
    private final boolean spin;
    //    private ServerSocketChannel serverSocketChannel;
    public final ConcurrentLinkedQueue taskQueue = new ConcurrentLinkedQueue();
    private final RingBuffer ringBuffer;
    private final FileDescriptor eventfd;
    private final LinuxSocket serverSocket;
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
    private final IntObjectMap<IOUringChannel> channels = new IntObjectHashMap<>(4096);

    public IOUringReactor(IOUringReactorFrontEnd frontend, Address thisAddress, int port, boolean spin) {
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
        this.serverSocket = LinuxSocket.newSocketStream(false);

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

    public Future<IOUringChannel> enqueue(SocketAddress address, Connection connection) {
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
        CompletableFuture<IOUringChannel> future;
    }

    @Override
    public void run() {
        setThreadAffinity();

        try {
            if (bind()) {
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

    private boolean bind() {
        InetSocketAddress address = null;
        try {
            address = new InetSocketAddress(thisAddress.getInetAddress(), port);
            //serverSocket.setTcpQuickAck(false);
            //serverSocket.setTcpNoDelay(true);
            serverSocket.bind(address);
            System.out.println(getName() + " Bind success " + address);
            serverSocket.listen(NetUtil.SOMAXCONN);
            System.out.println(getName() + " Listening on " + address);
            return true;
        } catch (IOException e) {
            logger.severe(getName() + " Could not bind to " + address);
        }
        return false;
    }

    private void eventLoop() throws Exception {
        sq.addEventFdRead(eventfd.intValue(), eventfdReadBuf, 0, 8, (short) 0);

        addAcceptRequest();

        while (!frontend.shuttingdown) {
            if (cq.hasCompletions()) {
                System.out.println(getName() + " has completions:" + cq.hasCompletions());
            }

            // Check there were any completion events to process
//            if (!cq.hasCompletions()) {
//                 sq.submitAndWait();
//            }

            //int processed = cq.process(this);
            //if (processed > 0) {
            //    System.out.println(getName() + " " + processed);
            //}

            Thread.sleep(10);
            processTasks();
        }
    }

    private void addAcceptRequest() {
        sq.addAccept(serverSocket.intValue(), acceptedAddressMemoryAddress, acceptedAddressLengthMemoryAddress, (short) 0);
    }

    @Override
    public void handle(int fd, int res, int flags, byte op, short data) {
        System.out.println(getName() + " handle called");

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
            System.out.println(getName() + " connectRequest address:" + address);

            LinuxSocket socket = LinuxSocket.newSocketStream(false);
            //socket.setTcpNoDelay(true);
            //socket.setTcpQuickAck(false);

            if (!socket.connect(connectRequest.address)) {
                connectRequest.future.completeExceptionally(new IOException("Could not connect to " + connectRequest.address));
                return;
            }

            IOUringChannel channel = new IOUringChannel();
            channel.reactor = this;
            channel.socket = socket;

            this.channels.put(socket.intValue(), channel);


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
