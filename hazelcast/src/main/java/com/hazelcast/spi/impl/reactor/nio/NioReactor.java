package com.hazelcast.spi.impl.reactor.nio;

import com.hazelcast.internal.networking.nio.SelectorOptimizer;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.spi.impl.reactor.Channel;
import com.hazelcast.spi.impl.reactor.ConnectRequest;
import com.hazelcast.spi.impl.reactor.frame.Frame;
import com.hazelcast.spi.impl.reactor.Reactor;
import com.hazelcast.spi.impl.reactor.SocketConfig;

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
import static com.hazelcast.spi.impl.reactor.frame.Frame.FLAG_OP_RESPONSE;
import static java.net.StandardSocketOptions.SO_RCVBUF;
import static java.nio.channels.SelectionKey.OP_ACCEPT;
import static java.nio.channels.SelectionKey.OP_READ;
import static java.nio.channels.SelectionKey.OP_WRITE;

public final class NioReactor extends Reactor {
    private final Selector selector;
    private final boolean spin;
    private final boolean writeThrough;
    private final AtomicBoolean wakeupNeeded = new AtomicBoolean(true);

    public NioReactor(NioReactorConfig config) {
        super(config);
        this.spin = config.spin;
        this.writeThrough = config.writeThrough;
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
    protected void eventLoop() throws Exception {
        while (running) {
            runTasks();

            boolean moreWork = scheduler.tick();

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

    private void handleSelectedKeys() {
        Iterator<SelectionKey> it = selector.selectedKeys().iterator();
        while (it.hasNext()) {
            SelectionKey key = it.next();
            it.remove();

            if (key.isValid() && key.isAcceptable()) {
                handleAccept(key);
            }

            if (key.isValid() && key.isReadable()) {
                handleRead(key);
            }

            if (key.isValid() && key.isWritable()) {
                handleWrite((NioChannel) key.attachment());
            }

            if (!key.isValid()) {
                key.cancel();
            }
        }
    }

    private void handleRead(SelectionKey key) {
        SocketChannel socketChannel = (SocketChannel) key.channel();
        NioChannel channel = (NioChannel) key.attachment();
        try {
            channel.readEvents.inc();
            ByteBuffer readBuf = channel.receiveBuffer;
            int bytesRead = socketChannel.read(readBuf);
            //System.out.println(this + " bytes read: " + bytesRead);
            if (bytesRead == -1) {
                channel.close();
                return;
            }

            channel.bytesRead.inc(bytesRead);
            readBuf.flip();
            Frame responseChain = null;
            for (; ; ) {
                Frame frame = channel.inboundFrame;
                if (frame == null) {
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
                    frame = channel.inboundFrame;
                    frame.byteBuffer().limit(size);
                    frame.writeInt(size);
                    frame.writeInt(flags);
                    frame.connection = channel.connection;
                    frame.channel = channel;
                }

                int size = frame.size();
                int remaining = size - frame.position();
                frame.write(readBuf, remaining);

                if (!frame.isComplete()) {
                    break;
                }

                frame.complete();
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
        } catch (IOException e) {
            channel.close();
            e.printStackTrace();
        }
    }

    @Override
    protected void handleWrite(Channel c) {
        NioChannel channel = (NioChannel) c;
        try {
            if (channel.flushThread.get() == null) {
                throw new RuntimeException("Channel is not in flushed state");
            }
            channel.handleWriteCnt.inc();

            IOVector ioVector = channel.ioVector;
            ioVector.fill(channel.unflushedFrames);
            long written = ioVector.write(channel.socketChannel);

            channel.bytesWritten.inc(written);
            //System.out.println(getName() + " bytes written:" + written);

            SelectionKey key = channel.key;
            if (ioVector.isEmpty()) {
                int interestOps = key.interestOps();
                if ((interestOps & OP_WRITE) != 0) {
                    key.interestOps(interestOps & ~OP_WRITE);
                }

                channel.resetFlushed();
            } else {
                System.out.println("Didn't manage to write everything." + channel);
                key.interestOps(key.interestOps() | OP_WRITE);
            }
        } catch (IOException e) {
            channel.close();
            e.printStackTrace();
        }
    }

    private void handleAccept(SelectionKey key) {
        NioServerChannel serverChannel = (NioServerChannel) key.attachment();
        try {
            SocketChannel socketChannel = serverChannel.serverSocketChannel.accept();
            socketChannel.configureBlocking(false);
            configure(socketChannel.socket(), serverChannel.socketConfig);

            SelectionKey channelSelectionKey = socketChannel.register(selector, OP_READ);
            channelSelectionKey.attach(newChannel(socketChannel, null, key, serverChannel.socketConfig));

            logger.info("Connection Accepted: " + socketChannel.getLocalAddress());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void registerAccept(InetSocketAddress serverAddress, SocketConfig socketConfig) throws IOException {
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.setOption(SO_RCVBUF, socketConfig.receiveBufferSize);
        System.out.println(getName() + " Binding to " + serverAddress);
        serverSocketChannel.bind(serverAddress);
        serverSocketChannel.configureBlocking(false);
        schedule(() -> {
            SelectionKey key = serverSocketChannel.register(selector, OP_ACCEPT);
            System.out.println(getName() + " ServerSocket listening at " + serverSocketChannel.getLocalAddress());

            NioServerChannel serverChannel = new NioServerChannel();
            serverChannel.socketConfig = socketConfig;
            serverChannel.serverSocketChannel = serverSocketChannel;
            key.attach(serverChannel);
        });
    }

    @Override
    protected void handleConnect(ConnectRequest request) {
        try {
            SocketAddress address = request.address;
            System.out.println("ConnectRequest address:" + address);

            SocketChannel socketChannel = SocketChannel.open();
            Socket socket = socketChannel.socket();
            configure(socket, request.socketConfig);

            // todo: call is blocking
            socketChannel.connect(address);
            socketChannel.configureBlocking(false);

            SelectionKey key = socketChannel.register(selector, OP_READ);

            NioChannel channel = newChannel(socketChannel, request.connection, key, request.socketConfig);
            key.attach(channel);

            logger.info("Socket listening at " + address);
            request.future.complete(channel);
        } catch (Exception e) {
            request.future.completeExceptionally(e);
        }
    }

    private NioChannel newChannel(SocketChannel socketChannel, Connection connection, SelectionKey key, SocketConfig socketConfig) throws IOException {
        System.out.println(this + " newChannel: " + socketChannel);

        NioChannel channel = new NioChannel();
        channel.reactor = this;
        channel.writeThrough = writeThrough;
        channel.key = key;
        channel.receiveBuffer = ByteBuffer.allocateDirect(socketConfig.receiveBufferSize);
        channel.socketChannel = socketChannel;
        channel.connection = connection;
        channel.remoteAddress = socketChannel.getRemoteAddress();
        channel.localAddress = socketChannel.getLocalAddress();
        channels.add(channel);
        return channel;
    }

    private void configure(Socket socket, SocketConfig socketConfig) throws SocketException {
        socket.setTcpNoDelay(socketConfig.tcpNoDelay);
        socket.setSendBufferSize(socketConfig.sendBufferSize);
        socket.setReceiveBufferSize(socketConfig.receiveBufferSize);

        String id = socket.getLocalAddress() + "->" + socket.getRemoteSocketAddress();
        System.out.println(getName() + " " + id + " tcpNoDelay: " + socket.getTcpNoDelay());
        System.out.println(getName() + " " + id + " receiveBufferSize: " + socket.getReceiveBufferSize());
        System.out.println(getName() + " " + id + " sendBufferSize: " + socket.getSendBufferSize());
    }

}
