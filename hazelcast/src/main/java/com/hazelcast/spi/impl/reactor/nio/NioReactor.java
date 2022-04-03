package com.hazelcast.spi.impl.reactor.nio;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.networking.nio.SelectorOptimizer;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.spi.impl.reactor.Channel;
import com.hazelcast.spi.impl.reactor.ChannelConfig;
import com.hazelcast.spi.impl.reactor.Reactor;
import com.hazelcast.spi.impl.reactor.ReactorFrontEnd;
import com.hazelcast.spi.impl.reactor.Invocation;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.nio.IOUtil.compactOrClear;
import static java.net.StandardSocketOptions.SO_RCVBUF;
import static java.nio.channels.SelectionKey.OP_READ;

public class NioReactor extends Reactor {
    private final Selector selector;
    private final boolean spin;
    private ServerSocketChannel serverSocketChannel;
    public final ConcurrentLinkedQueue runQueue = new ConcurrentLinkedQueue();
    public final AtomicBoolean wakeupNeeded = new AtomicBoolean(true);

    public NioReactor(ReactorFrontEnd frontend, ChannelConfig channelConfig, Address thisAddress, int port, boolean spin) {
        super(frontend, channelConfig, thisAddress, port,
                "NioReactor:[" + thisAddress.getHost() + ":" + thisAddress.getPort() + "]:" + port);

        this.spin = spin;
        this.selector = SelectorOptimizer.newSelector(logger);
    }

    public void wakeup() {
        if (spin || Thread.currentThread() == this) {
            return;
        }

        if (wakeupNeeded.get() && wakeupNeeded.compareAndSet(true, false)) {
            selector.wakeup();
        }
    }

    @Override
    public void schedule(Invocation request) {
        runQueue.add(request);
        wakeup();
    }

    public void schedule(Channel channel) {
        runQueue.add(channel);
        wakeup();
    }

    @Override
    protected void schedule(ConnectRequest request) {
        runQueue.add(request);
        wakeup();
    }

    @Override
    public void setupServerSocket() throws Exception {
        serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.setOption(SO_RCVBUF, channelConfig.receiveBufferSize);

        InetSocketAddress serverAddress = new InetSocketAddress(thisAddress.getInetAddress(), port);
        System.out.println(getName() + " Binding to " + serverAddress);
        serverSocketChannel.bind(serverAddress);
        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        System.out.println(getName() + " ServerSocket listening at " + serverAddress);
    }

    private NioChannel newChannel(SocketChannel socketChannel, Connection connection) throws IOException {
        System.out.println(this + " newChannel: " + socketChannel);

        NioChannel channel = new NioChannel();
        channel.reactor = this;
        channel.receiveBuffer = ByteBuffer.allocateDirect(channelConfig.receiveBufferSize);
        channel.socketChannel = socketChannel;
        channel.connection = connection;
        channel.remoteAddress = socketChannel.getRemoteAddress();
        channel.localAddress = socketChannel.getLocalAddress();
        channels.add(channel);
        return channel;
    }

    @Override
    public void eventLoop() throws Exception {
        while (!frontend.shuttingdown) {
            runTasks();

            int keyCount;
            if (spin) {
                keyCount = selector.selectNow();
            } else {
                wakeupNeeded.set(true);
                if (runQueue.isEmpty()) {
                    keyCount = selector.select();
                } else {
                    keyCount = selector.selectNow();
                }
                wakeupNeeded.set(false);
            }

            if (keyCount > 0) {
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
        }
    }

    private void runTasks() {
        for (; ; ) {
            Object task = runQueue.poll();
            if (task == null) {
                break;
            }

            if (task instanceof NioChannel) {
                handleOutbound((NioChannel) task);
            } else if (task instanceof Invocation) {
                handleLocalOp((Invocation) task);
            } else if (task instanceof ConnectRequest) {
                handleConnectRequest((ConnectRequest) task);
            } else {
                throw new RuntimeException("Unrecognized type:" + task.getClass());
            }
        }
    }

    private void handleRead(SelectionKey key) throws IOException {
        SocketChannel socketChannel = (SocketChannel) key.channel();
        NioChannel channel = (NioChannel) key.attachment();
        channel.readEvents++;
        ByteBuffer readBuf = channel.receiveBuffer;
        int bytesRead = socketChannel.read(readBuf);
        //System.out.println(this + " bytes read: " + bytesRead);
        if (bytesRead == -1) {
            socketChannel.close();
            key.cancel();
            return;
        }

        channel.bytesRead += bytesRead;
        readBuf.flip();
        Packet responseChain = null;
        for (; ; ) {
            Packet packet = channel.packetReader.readFrom(channel.receiveBuffer);
            //System.out.println(this + " read packet: " + packet);
            if (packet == null) {
                break;
            }

            channel.packetsRead++;
            packet.setConn((ServerConnection) channel.connection);
            packet.channel = channel;

            if (packet.isFlagRaised(Packet.FLAG_OP_RESPONSE)) {
                packet.next = responseChain;
                responseChain = packet;
                //frontend.handleResponse(packet);
            } else {
                handleRemoteOp(packet);
            }
        }
        compactOrClear(readBuf);

        if (responseChain != null) {
            frontend.handleResponse(responseChain);
        }

        // unwanted outbound in case of only responses.
        if (!channel.scheduled.get() && channel.scheduled.compareAndSet(false, true)) {
            handleOutbound(channel);
        }
    }

    private void handleAccept() throws IOException {
        SocketChannel socketChannel = serverSocketChannel.accept();
        socketChannel.configureBlocking(false);
        configure(socketChannel.socket());

        SelectionKey selectionKey = socketChannel.register(selector, OP_READ);
        selectionKey.attach(newChannel(socketChannel, null));

        logger.info("Connection Accepted: " + socketChannel.getLocalAddress());
    }

    private void handleOutbound(NioChannel channel) {
        channel.handleOutboundCalls++;
        //System.out.println("Processing channel");
        try {
            for (; ; ) {
                ByteBuffer buffer = channel.pending.poll();
                if (buffer == null) {
                    break;
                }

                channel.addBuffer(buffer);
            }

            long written = channel.socketChannel.write(channel.buffs, 0, channel.buffsLen);
            //System.out.println(getName()+" bytes written:"+written);
            channel.bytesWritten += written;

            channel.discardWrittenBuffers();

            // todo: we should only unschedule if there is nothing left to write.
            channel.unschedule();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void handleConnectRequest(ConnectRequest request) {
        try {
            SocketAddress address = request.address;
            System.out.println("ConnectRequest address:" + address);

            SocketChannel socketChannel = SocketChannel.open();
            Socket socket = socketChannel.socket();
            configure(socket);

            // todo: call is blocking
            socketChannel.connect(address);
            socketChannel.configureBlocking(false);

            SelectionKey key = socketChannel.register(selector, OP_READ);

            NioChannel channel = newChannel(socketChannel, request.connection);
            key.attach(channel);

            logger.info("Socket listening at " + address);
            request.future.complete(channel);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void configure(Socket socket) throws SocketException {
        socket.setTcpNoDelay(channelConfig.tcpNoDelay);
        socket.setSendBufferSize(channelConfig.sendBufferSize);
        socket.setReceiveBufferSize(channelConfig.receiveBufferSize);

        String id = socket.getLocalAddress() + "->" + socket.getRemoteSocketAddress();
        System.out.println(getName() + " " + id + " tcpNoDelay: " + socket.getTcpNoDelay());
        System.out.println(getName() + " " + id + " receiveBufferSize: " + socket.getReceiveBufferSize());
        System.out.println(getName() + " " + id + " sendBufferSize: " + socket.getSendBufferSize());
    }
}
