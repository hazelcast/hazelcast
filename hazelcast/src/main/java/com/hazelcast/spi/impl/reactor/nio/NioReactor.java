package com.hazelcast.spi.impl.reactor.nio;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.networking.nio.SelectorOptimizer;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.spi.impl.reactor.Channel;
import com.hazelcast.spi.impl.reactor.ChannelConfig;
import com.hazelcast.spi.impl.reactor.Frame;
import com.hazelcast.spi.impl.reactor.Reactor;
import com.hazelcast.spi.impl.reactor.ReactorFrontEnd;

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
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.IOUtil.compactOrClear;
import static com.hazelcast.internal.nio.Packet.FLAG_OP_RESPONSE;
import static java.net.StandardSocketOptions.SO_RCVBUF;
import static java.nio.channels.SelectionKey.OP_READ;

public final class NioReactor extends Reactor {
    private final Selector selector;
    private final boolean spin;
    private ServerSocketChannel serverSocketChannel;
    private final AtomicBoolean wakeupNeeded = new AtomicBoolean(true);

    public NioReactor(ReactorFrontEnd frontend,
                      ChannelConfig channelConfig,
                      Address thisAddress,
                      int port,
                      boolean spin,
                      boolean poolRequests,
                      boolean poolResponses) {
        super(frontend, channelConfig, thisAddress, port,
                "NioReactor:[" + thisAddress.getHost() + ":" + thisAddress.getPort() + "]:" + port,
                poolRequests, poolResponses);

        this.spin = spin;
        this.selector = SelectorOptimizer.newSelector(logger);
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
    protected void setupServerSocket() throws Exception {
        serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.setOption(SO_RCVBUF, channelConfig.receiveBufferSize);

        InetSocketAddress serverAddress = new InetSocketAddress(thisAddress.getInetAddress(), port);
        // System.out.println(getName() + " Binding to " + serverAddress);
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

    private void configure(Socket socket) throws SocketException {
        socket.setTcpNoDelay(channelConfig.tcpNoDelay);
        socket.setSendBufferSize(channelConfig.sendBufferSize);
        socket.setReceiveBufferSize(channelConfig.receiveBufferSize);

        String id = socket.getLocalAddress() + "->" + socket.getRemoteSocketAddress();
        System.out.println(getName() + " " + id + " tcpNoDelay: " + socket.getTcpNoDelay());
        System.out.println(getName() + " " + id + " receiveBufferSize: " + socket.getReceiveBufferSize());
        System.out.println(getName() + " " + id + " sendBufferSize: " + socket.getSendBufferSize());
    }

    @Override
    protected void eventLoop() throws Exception {
        while (!frontend.shuttingdown) {
            runTasks();

            boolean moreWork = scheduler.tick();
            //todo: dirty channels are not scheduled here.

            flushDirtyChannels();

            int keyCount;
            if (spin || moreWork) {
                keyCount = selector.selectNow();
            } else {
                wakeupNeeded.set(true);
                if (publicRunQueue.isEmpty()) {
                    keyCount = selector.select();
                } else {
                    keyCount = selector.selectNow();
                }
                wakeupNeeded.set(false);
            }

            if (keyCount > 0) {
                handleSelectedKeys();
            }
        }
    }

    private void handleSelectedKeys() throws IOException {
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
        channel.readEvents.inc();
        ByteBuffer readBuf = channel.receiveBuffer;
        int bytesRead = socketChannel.read(readBuf);
        //System.out.println(this + " bytes read: " + bytesRead);
        if (bytesRead == -1) {
            socketChannel.close();
            key.cancel();
            return;
        }

        channel.bytesRead.inc(bytesRead);
        readBuf.flip();
        Frame responseChain = null;
        for (; ; ) {
            if (channel.inboundFrame == null) {
                if (readBuf.remaining() < INT_SIZE_IN_BYTES + INT_SIZE_IN_BYTES) {
                    break;
                }

                int size = readBuf.getInt();
                int flags = readBuf.getInt();
                if ((flags & FLAG_OP_RESPONSE) == 0) {
                    channel.inboundFrame = requestFrameAllocator.allocate(size);
                } else {
                    channel.inboundFrame = remoteResponseFrameAllocator.allocate(size);
                }

                channel.inboundFrame.writeInt(size);
                channel.inboundFrame.writeInt(flags);
                channel.inboundFrame.connection = channel.connection;
                channel.inboundFrame.channel = channel;
            }

            // todo: we need to copy.
            int size = channel.inboundFrame.size();
            int remaining = size - channel.inboundFrame.position();
            channel.inboundFrame.write(readBuf, remaining);

            if (!channel.inboundFrame.isComplete()) {
                break;
            }

            channel.inboundFrame.complete();
            Frame frame = channel.inboundFrame;
            channel.inboundFrame = null;
            channel.framesRead.inc();

            if (frame.isFlagRaised(FLAG_OP_RESPONSE)) {
                frame.next = responseChain;
                responseChain = frame;
            } else {
                handleRequest(frame);
            }
        }
        compactOrClear(readBuf);

        if (responseChain != null) {
            frontend.handleResponse(responseChain);
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

    @Override
    protected void handleOutbound(Channel c) {
        // Either an public write could have triggered this
        // But also internal read of the socket could have triggered this.

        // So how can we prevent

        NioChannel channel = (NioChannel) c;

        if (channel.flushThread.get() == null) {
            throw new RuntimeException("Channel is not in flushed state");
        }

        channel.handleOutboundCalls.inc();
        //System.out.println("Processing channel");
        for (; ; ) {
            Frame frame = channel.unflushedFrames.poll();
            if (frame == null) {
                break;
            }

            channel.addFlushedFrame(frame);
        }

        try {
            long written = channel.socketChannel.write(channel.buffs, 0, channel.buffsLen);
            channel.bytesWritten.inc(written);
            //  System.out.println(getName() + " bytes written:" + written);

            channel.discardWrittenBuffers();

            // todo: we should only unschedule if there is nothing left to write.
            // otherwise we need to schedule for a OP_WRITE
            channel.resetFlushed();
        } catch (IOException e) {
            //todo: we need to get rid of the channel.
            e.printStackTrace();
        }
    }

    @Override
    protected void handleConnectRequest(ConnectRequest request) {
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
}
