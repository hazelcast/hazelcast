package com.hazelcast.spi.impl.reactor.nio;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.networking.nio.SelectorOptimizer;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.serialization.impl.ByteArrayObjectDataInput;
import com.hazelcast.internal.serialization.impl.ByteArrayObjectDataOutput;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.reactor.*;
import com.hazelcast.table.impl.SelectByKeyOperation;
import com.hazelcast.table.impl.UpsertOperation;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.BitSet;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.nio.IOUtil.compactOrClear;
import static com.hazelcast.spi.impl.reactor.Op.RUN_CODE_DONE;
import static com.hazelcast.spi.impl.reactor.Op.RUN_CODE_FOO;
import static com.hazelcast.spi.impl.reactor.OpCodes.TABLE_SELECT_BY_KEY;
import static com.hazelcast.spi.impl.reactor.OpCodes.TABLE_UPSERT;
import static java.nio.channels.SelectionKey.OP_READ;

public class NioReactor extends Reactor {
    private final Selector selector;
    private final boolean spin;
    private ServerSocketChannel serverSocketChannel;
    public final ConcurrentLinkedQueue taskQueue = new ConcurrentLinkedQueue();
    private final AtomicBoolean wakeupNeeded = new AtomicBoolean();
    private long nextPrint = System.currentTimeMillis() + 1000;

    public NioReactor(ReactorFrontEnd frontend, Address thisAddress, int port, boolean spin) {
        super(frontend, thisAddress, port,
                "NioReactor:[" + thisAddress.getHost() + ":" + thisAddress.getPort() + "]:" + port);

        this.spin = spin;
        this.selector = SelectorOptimizer.newSelector(frontend.logger);
    }

    @Override
    public void wakeup() {
        if (spin || Thread.currentThread() == this) {
            return;
        }

        if (wakeupNeeded.get() && wakeupNeeded.compareAndSet(true, false)) {
            selector.wakeup();
        }
    }

    @Override
    public void enqueue(Request request) {
        taskQueue.add(request);
        wakeup();
    }

    @Override
    public Future<Channel> enqueue(SocketAddress address, Connection connection) {
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
        CompletableFuture<Channel> future;
    }

    @Override
    public void executeRun() {
        try {
            if (setupServerSocket()) {
                eventLoop();
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }

        System.out.println(getName() + " Died: frontend shutting down:" + frontend.shuttingdown);
    }

    private boolean setupServerSocket() {
        InetSocketAddress serverAddress = null;
        try {
            serverSocketChannel = ServerSocketChannel.open();
            serverAddress = new InetSocketAddress(thisAddress.getInetAddress(), port);
            System.out.println(getName() + " Binding to " + serverAddress);
            serverSocketChannel.bind(serverAddress);
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
            logger.info(getName() + " ServerSocket listening at " + serverAddress);
            return true;
        } catch (IOException e) {
            logger.severe(getName() + " Could not bind to " + serverAddress);
        }
        return false;
    }

    private NioChannel newChannel(SocketChannel socketChannel, Connection connection) {
        System.out.println(this + " newChannel: " + socketChannel);

        NioChannel channel = new NioChannel();
        channel.reactor = this;
        channel.readBuffer = ByteBuffer.allocate(256 * 1024);
        channel.socketChannel = socketChannel;
        channel.connection = connection;
        return channel;
    }

    private void eventLoop() throws Exception {
        while (!frontend.shuttingdown) {
            int keyCount;
            if (spin) {
                keyCount = selector.selectNow();
            } else {
                wakeupNeeded.set(true);
                keyCount = selector.select();
                wakeupNeeded.set(false);
            }


            //System.out.println(this + " selectionCount:" + keyCount);

            if (keyCount > 0) {
                handleSelectionKeys();
            }

            processTasks();
        }
    }

    private void printChannelsDebug() {
        if (System.currentTimeMillis() > nextPrint) {
            for (SelectionKey key : selector.keys()) {
                NioChannel channel = (NioChannel) key.attachment();
                if (channel != null) {
                    System.out.println(channel.toDebugString());
                }
            }
            nextPrint = System.currentTimeMillis() + 1000;
        }
    }

    private void handleSelectionKeys() throws IOException {
        Iterator<SelectionKey> it = selector.selectedKeys().iterator();
        while (it.hasNext()) {
            SelectionKey key = it.next();
            it.remove();

            if (key.isValid() && key.isAcceptable()) {
                handleAccept();
            }

            if (key.isValid() && key.isReadable()) {
                handleRead(key);
            }

            if (!key.isValid()) {
                //System.out.println("sk not valid");
                key.cancel();
            }
        }
    }

    private void handleRead(SelectionKey key) throws IOException {
        SocketChannel socketChannel = (SocketChannel) key.channel();
        NioChannel channel = (NioChannel) key.attachment();
        ByteBuffer readBuf = channel.readBuffer;
        int bytesRead = socketChannel.read(readBuf);
        //System.out.println(this + " bytes read: " + bytesRead);
        if (bytesRead == -1) {
            socketChannel.close();
            key.cancel();
            return;
        }

        channel.bytesRead += bytesRead;
        readBuf.flip();
        for (; ; ) {
            Packet packet = channel.packetReader.readFrom(channel.readBuffer);
            //System.out.println(this + " read packet: " + packet);
            if (packet == null) {
                break;
            }

            channel.packetsRead++;
            packet.setConn((ServerConnection) channel.connection);
            packet.channel = channel;
            process(packet);
        }
        compactOrClear(readBuf);
    }

    private void handleAccept() throws IOException {
        SocketChannel socketChannel = serverSocketChannel.accept();
        socketChannel.configureBlocking(false);
        socketChannel.socket().setTcpNoDelay(true);
        SelectionKey selectionKey = socketChannel.register(selector, OP_READ);
        selectionKey.attach(newChannel(socketChannel, null));

        logger.info("Connection Accepted: " + socketChannel.getLocalAddress());
    }

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
            System.out.println("ConnectRequest address:" + address);

            SocketChannel socketChannel = SocketChannel.open();
            // todo: call is blocking
            socketChannel.connect(address);
            socketChannel.socket().setTcpNoDelay(true);
            socketChannel.configureBlocking(false);
            SelectionKey key = socketChannel.register(selector, OP_READ);

            NioChannel channel = newChannel(socketChannel, connectRequest.connection);
            key.attach(channel);

            logger.info("Socket listening at " + address);
            connectRequest.future.complete(channel);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

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
