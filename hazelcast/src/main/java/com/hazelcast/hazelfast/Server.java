package com.hazelcast.hazelfast;


import com.hazelcast.client.impl.CompositeMessageTaskFactory;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.task.AbstractPartitionMessageTask;
import com.hazelcast.client.impl.protocol.task.MessageTask;
import com.hazelcast.client.impl.protocol.util.ClientProtocolBuffer;
import com.hazelcast.client.impl.protocol.util.SafeBuffer;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.logging.ILogger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.hazelfast.IOUtil.INT_AS_BYTES;
import static com.hazelcast.hazelfast.IOUtil.allocateByteBuffer;
import static com.hazelcast.hazelfast.IOUtil.compactOrClear;
import static com.hazelcast.hazelfast.IOUtil.newSelector;
import static com.hazelcast.hazelfast.IOUtil.setReceiveBufferSize;
import static com.hazelcast.hazelfast.IOUtil.setSendBufferSize;
import static java.nio.channels.SelectionKey.OP_ACCEPT;
import static java.nio.channels.SelectionKey.OP_READ;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Ways to connect:
 * - each server thread has its own IO port.
 * - then the client needs to know how many IO threads there are per server.
 * This will not increase latency since connecting can be done in parallel
 * This requires more configuration. Server can't decide without telling the client
 * how many IO threads there are.
 * - shared io port for all server threads; but how does a client then identify the IO thread it wants to register to?
 * - then the server could tell to the client the number of remaining connections are needed.
 * This will increase latency since multiple round trips are needed.
 * This will require less configuration. Server will client how to complete the handshake.
 */
public class Server {

    private final Node node;
    private ServerSocketChannel serverSocket;
    private InetSocketAddress serverAddress;
    private final PartitionThread[] partitionThreads;
    private final AcceptThread acceptThread;
    private final AtomicInteger nextPartitionThread = new AtomicInteger();
    private volatile boolean stopping = false;
    private final String bindAddress;
    private final int port;
    private final int receiveBufferSize;
    private final int sendBufferSize;
    private final boolean tcpNoDelay;
    private final boolean objectPoolingEnabled;
    private final boolean optimizeSelector;
    private final boolean directBuffers;
    private final boolean selectorSpin;
    private final long selectorSpinDurationNs;
    private final CompositeMessageTaskFactory messageTaskFactory;
    private final ILogger logger;

    public Server(Context context) {
        this.messageTaskFactory = new CompositeMessageTaskFactory(context.node.nodeEngine);
        this.bindAddress = context.bindAddress;
        this.port = context.startPort;
        this.receiveBufferSize = context.receiveBufferSize;
        this.sendBufferSize = context.sendBufferSize;
        this.tcpNoDelay = context.tcpNoDelay;
        this.objectPoolingEnabled = context.objectPoolingEnabled;
        this.optimizeSelector = context.optimizeSelector;
        this.directBuffers = context.directBuffers;
        this.selectorSpin = context.selectorSpin;
        this.selectorSpinDurationNs = context.selectorSpinDurationNs;
        this.node = context.node;
        this.logger = context.logger;
        this.partitionThreads = new PartitionThread[context.partitionThreadCount];
        for (int k = 0; k < partitionThreads.length; k++) {
            partitionThreads[k] = new PartitionThread(k);
        }
        this.acceptThread = new AcceptThread();
    }

    public String hostname() {
        return bindAddress;
    }

    public int port() {
        return port;
    }

    public void stop() throws IOException {
        System.out.println("Stopping Server");
        stopping = true;
        serverSocket.close();
        acceptThread.shutdown();
        for (PartitionThread serverThread : partitionThreads) {
            serverThread.shutdown();
        }
    }

    public void start() throws IOException {
        serverSocket = ServerSocketChannel.open();
        serverSocket.socket().setReuseAddress(true);
        serverAddress = new InetSocketAddress(bindAddress, port);
        logger.info("serverAddress:" + serverAddress);
        logger.info("serverThreadCount:" + partitionThreads);
        serverSocket.bind(serverAddress);

        serverSocket.socket().setReceiveBufferSize(receiveBufferSize);
        if (serverSocket.socket().getReceiveBufferSize() != receiveBufferSize) {
            System.out.println("socket doesn't have expected receiveBufferSize, expected:"
                    + receiveBufferSize + " actual:" + serverSocket.socket().getReceiveBufferSize());
        }
        serverSocket.configureBlocking(false);

        for (PartitionThread t:partitionThreads) {
            t.start();
        }
        acceptThread.start();
    }

    public class PartitionThread extends Thread {
        private final Selector selector;
        private final ConcurrentLinkedQueue<SocketChannel> newChannels = new ConcurrentLinkedQueue<>();

        private PartitionThread(int id) {
            super("PartitionThread-" + id);
            this.setDaemon(true);
            this.selector = newSelector(optimizeSelector);
        }

        @Override
        public void run() {
            logger.info(getName() + " running");
            try {
                selectLoop();
            } catch (Exception e) {
                if (!stopping) {
                    logger.warning(e);
                }
            }
        }

        private void selectLoop() throws IOException {
            long spinEndNs = System.nanoTime() + selectorSpinDurationNs;
            for (; ; ) {
                int selectedKeys = (selectorSpin && spinEndNs > System.nanoTime()) ? selector.selectNow() : selector.select();
                registerNewChannels();
                if (selectedKeys == 0) {
                    continue;
                }

                Iterator<SelectionKey> it = selector.selectedKeys().iterator();
                while (it.hasNext()) {
                    SelectionKey sk = it.next();
                    it.remove();

                    try {
                        if (sk.isReadable()) {
                            onRead(sk);
                        }
                        if (sk.isWritable()) {
                            onWrite(sk);
                        }
                    } catch (Throwable e) {
                        logger.warning(e);
                        sk.channel().close();
                    }
                }
                if (selectorSpin) {
                    spinEndNs = System.nanoTime() + selectorSpinDurationNs;
                }
            }
        }

        private void registerNewChannels() throws IOException {
            for (; ; ) {
                SocketChannel channel = newChannels.poll();
                if (channel == null) {
                    break;
                }

                channel.configureBlocking(false);
                Socket socket = channel.socket();
                socket.setTcpNoDelay(tcpNoDelay);
                SupportConnection con = new SupportConnection(objectPoolingEnabled);
                setReceiveBufferSize(channel, receiveBufferSize);
                con.receiveBuf = allocateByteBuffer(directBuffers, socket.getReceiveBufferSize());
                setSendBufferSize(channel, sendBufferSize);
                con.sendBuf = allocateByteBuffer(directBuffers, socket.getSendBufferSize());
                con.channel = channel;
                channel.register(selector, OP_READ, con);
            }
        }

        private void onWrite(SelectionKey sk) throws IOException {
            SocketChannel channel = (SocketChannel) sk.channel();
            SupportConnection con = (SupportConnection) sk.attachment();
            // todo: this field is increased even if we are triggered from the onRead
            con.onWriteEvents++;

            for (; ; ) {
                if (con.sendFrame == null) {
                    // check if there is enough space to writeAndFlush the length
                    if (con.sendBuf.remaining() < INT_AS_BYTES) {
                        break;
                    }

                    con.sendFrame = con.pending.poll();
                    if (con.sendFrame == null) {
                        break;
                    }
                    con.sendBuf.putInt(con.sendFrame.length);
                }

                int missingFromFrame = con.sendFrame.length - con.sendOffset;
                int length;
                boolean complete;
                if (con.sendBuf.remaining() <= missingFromFrame) {
                    length = con.sendBuf.remaining();
                    complete = false;
                } else {
                    length = missingFromFrame;
                    complete = true;
                }
                con.sendBuf.put(con.sendFrame.bytes, con.sendOffset, length);

                if (complete) {
                   con.byteArrayPool.returnToPool(con.sendFrame.bytes);
                    con.framePool.returnToPool(con.sendFrame);
                    con.sendFrame = null;
                    con.sendOffset = 0;
                } else {
                    con.sendOffset += length;
                    break;
                }
            }
            con.sendBuf.flip();
            long bytesWritten = channel.write(con.sendBuf);

            con.bytesWritten += bytesWritten;

            if (con.sendBuf.remaining() == 0 && con.sendFrame == null) {
                // unregister
                int interestOps = sk.interestOps();
                if ((interestOps & SelectionKey.OP_WRITE) != 0) {
                    sk.interestOps(interestOps & ~SelectionKey.OP_WRITE);
                }
            } else {
                // register OP_WRITE
                sk.interestOps(sk.interestOps() | SelectionKey.OP_WRITE);
            }

            compactOrClear(con.sendBuf);
        }

        private void onRead(SelectionKey sk) throws IOException {
            SocketChannel channel = (SocketChannel) sk.channel();
            SupportConnection con = (SupportConnection) sk.attachment();
            con.onReadEvents++;

            int bytesRead = channel.read(con.receiveBuf);
            if (bytesRead == -1) {
                throw new IOException("Channel " + channel.socket().getInetAddress() + " closed on the other side");
            }
            con.bytesRead += bytesRead;

            con.receiveBuf.flip();
            boolean dirty = false;
            try {
                while (con.receiveBuf.remaining() > 0) {
                    if (con.receiveFrame == null) {
                        // not enough bytes available for the frame size; we are done.
                        if (con.receiveBuf.remaining() < INT_AS_BYTES) {
                            break;
                        }

                        con.receiveFrame = con.framePool.takeFromPool();
                        con.receiveFrame.length = con.receiveBuf.getInt();
                        if (con.receiveFrame.length < 0) {
                            throw new IOException("Frame length can't be negative. Found:" + con.receiveFrame.length);
                        }

                        con.receiveFrame.bytes = con.byteArrayPool.takeFromPool(con.receiveFrame.length);
                    }

                    int missingFromFrame = con.receiveFrame.length - con.receiveOffset;
                    int bytesToRead = con.receiveBuf.remaining() < missingFromFrame ? con.receiveBuf.remaining() : missingFromFrame;

                    con.receiveBuf.get(con.receiveFrame.bytes, con.receiveOffset, bytesToRead);
                    con.receiveOffset += bytesToRead;
                    if (con.receiveOffset == con.receiveFrame.length) {
                        // we have fully loaded a frame.
                        dirty = true;
                        con.readFrames++;
                        byte[] response = process(con.receiveFrame.bytes);
                        Frame responseFrame = new Frame();
                        responseFrame.bytes = response;
                        responseFrame.length = response.length;
                        con.pending.add(responseFrame);
                        con.receiveFrame = null;
                        con.receiveOffset = 0;
                    }
                }
            } finally {
                compactOrClear(con.receiveBuf);
            }

            if (dirty) {
                onWrite(sk);
            }
        }

        private byte[] process(byte[] request) {
            ClientProtocolBuffer buffer = new SafeBuffer(request);
            ClientMessage requestMsg = ClientMessage.createForDecode(buffer, 0);

            MessageTask messageTask = messageTaskFactory.create(requestMsg, null);
            if (!(messageTask instanceof AbstractPartitionMessageTask)) {
                throw new RuntimeException("Only partition based message tasks are supported via support connections ");
            }
            AbstractPartitionMessageTask task = (AbstractPartitionMessageTask) messageTask;
            ClientMessage response = task.runBlocking();
            return response.buffer().byteArray();
        }

        private void shutdown() {
            acceptThread.interrupt();
            try {
                acceptThread.selector.close();
            } catch (IOException e) {
            }
        }
    }

    private class AcceptThread extends Thread {
        private Selector selector;

        AcceptThread() {
            super("AcceptThread");
        }

        @Override
        public void run() {
            try {
                loop();
            } catch (Exception e) {
                if (!stopping) {
                    logger.warning(e);
                }
            }
        }

        private void shutdown() {
            acceptThread.interrupt();
            try {
                acceptThread.selector.close();
            } catch (IOException e) {
            }
        }

        private void loop() throws IOException {
            selector = Selector.open();
            serverSocket.register(selector, OP_ACCEPT, null);
            for (; ; ) {
                selector.select();
                Set<SelectionKey> selectedKeys = selector.selectedKeys();
                Iterator<SelectionKey> iterator = selectedKeys.iterator();

                while (iterator.hasNext()) {
                    SelectionKey sk = iterator.next();
                    iterator.remove();

                    if (sk.isAcceptable()) {
                        onAccept();
                    }
                }
            }
        }

        private void onAccept() throws IOException {
            SocketChannel clientChannel = serverSocket.accept();
            logger.info("Accepted: " + clientChannel.getLocalAddress());
            PartitionThread ioThread = nextPartitionThread();
            ioThread.newChannels.add(clientChannel);
            ioThread.selector.wakeup();
        }

        private PartitionThread nextPartitionThread() {
            int next = nextPartitionThread.getAndIncrement() % partitionThreads.length;
            return partitionThreads[next];
        }
    }

    public static class Context {
        private int partitionThreadCount = Math.max(4, 36);//Runtime.getRuntime().availableProcessors()/2);

        private String bindAddress = "0.0.0.0";
        private ILogger logger;
        private int startPort = 1111;
        private int receiveBufferSize = 256 * 1024;
        private int sendBufferSize = 256 * 1024;
        private boolean tcpNoDelay = true;
        // pooling currently doesn't work because the byte array returned by a ClientMessage, can't be
        // obtained from the buffer pool.
        private boolean objectPoolingEnabled = false;
        private boolean optimizeSelector = true;
        private boolean directBuffers = true;
        private boolean selectorSpin = false;
        private long selectorSpinDurationNs = MILLISECONDS.toNanos(10);
        private Node node;

        public Context selectorSpinDuration(long duration, TimeUnit unit) {
            this.selectorSpinDurationNs = unit.toNanos(duration);
            return this;
        }

        public Context selectorSpin(boolean selectorSpin) {
            this.selectorSpin = selectorSpin;
            return this;
        }

        public Context partitionThreadCount(int partitionThreadCount) {
            this.partitionThreadCount = partitionThreadCount;
            return this;
        }

        public Context bindAddress(String bindAddress) {
            this.bindAddress = bindAddress;
            return this;
        }

        public Context startPort(int startPort) {
            this.startPort = startPort;
            return this;
        }

        public Context receiveBufferSize(int receiveBufferSize) {
            this.receiveBufferSize = receiveBufferSize;
            return this;
        }

        public Context sendBufferSize(int sendBufferSize) {
            this.sendBufferSize = sendBufferSize;
            return this;
        }

        public Context tcpNoDelay(boolean tcpNoDelay) {
            this.tcpNoDelay = tcpNoDelay;
            return this;
        }

        public Context objectPoolingEnabled(boolean objectPoolingEnabled) {
            this.objectPoolingEnabled = objectPoolingEnabled;
            return this;
        }

        public Context optimizeSelector(boolean optimizeSelector) {
            this.optimizeSelector = optimizeSelector;
            return this;
        }

        public Context directBuffers(boolean directBuffers) {
            this.directBuffers = directBuffers;
            return this;
        }

        public Context node(Node node) {
            this.node = node;
            this.logger = node.getLogger(Server.class);
            return this;
        }
    }
}
