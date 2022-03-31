package com.hazelcast.spi.impl.reactor.nio;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.networking.nio.SelectorOptimizer;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.spi.impl.reactor.Channel;
import com.hazelcast.spi.impl.reactor.Op;
import com.hazelcast.spi.impl.reactor.Reactor;
import com.hazelcast.spi.impl.reactor.ReactorFrontEnd;
import com.hazelcast.spi.impl.reactor.Request;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.nio.IOUtil.compactOrClear;
import static java.nio.channels.SelectionKey.OP_READ;

public class NioReactor extends Reactor {
    private final Selector selector;
    private final boolean spin;
    private ServerSocketChannel serverSocketChannel;
    public final ConcurrentLinkedQueue runQueue = new ConcurrentLinkedQueue();
    public final AtomicBoolean wakeupNeeded = new AtomicBoolean(true);
    private long nextPrint = System.currentTimeMillis() + 1000;

    public NioReactor(ReactorFrontEnd frontend, Address thisAddress, int port, boolean spin) {
        super(frontend, thisAddress, port,
                "NioReactor:[" + thisAddress.getHost() + ":" + thisAddress.getPort() + "]:" + port);

        this.spin = spin;
        this.selector = SelectorOptimizer.newSelector(frontend.logger);
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
    public void schedule(Request request) {
        runQueue.add(request);
        wakeup();
    }

    public void schedule(Channel channel) {
        runQueue.add(channel);
        wakeup();
    }

    @Override
    public Future<Channel> asyncConnect(SocketAddress address, Connection connection) {
        logger.info("Connect to " + address);

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
        serverSocketChannel = ServerSocketChannel.open();
        InetSocketAddress serverAddress = new InetSocketAddress(thisAddress.getInetAddress(), port);
        System.out.println(getName() + " Binding to " + serverAddress);
        serverSocketChannel.bind(serverAddress);
        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        System.out.println(getName() + " ServerSocket listening at " + serverAddress);
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

    @Override
    public void eventLoop() throws Exception {
        while (!frontend.shuttingdown) {
            //Thread.sleep(100);
            //System.out.println(this+" eventLoop");

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

            for (; ; ) {
                Object task = runQueue.poll();
                if (task == null) {
                    break;
                }

                if (task instanceof NioChannel) {
                    handleChannel((NioChannel) task);
                } else if (task instanceof Op) {
                    handleOp((Op) task);
                } else if (task instanceof Request) {
                    handleRequest((Request) task);
                } else if (task instanceof ConnectRequest) {
                    handleConnectRequest((ConnectRequest) task);
                } else {
                    throw new RuntimeException("Unrecognized type:" + task.getClass());
                }
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
            handlePacket(packet);
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

    private void handleChannel(NioChannel channel) {
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
            //System.out.println(getName()+" bytes written:"+written);
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
            // todo: call is blocking
            socketChannel.connect(address);
            socketChannel.socket().setTcpNoDelay(true);
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
