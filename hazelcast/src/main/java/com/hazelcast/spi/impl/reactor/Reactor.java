package com.hazelcast.spi.impl.reactor;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.networking.nio.SelectorOptimizer;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.nio.PacketIOHelper;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.ByteArrayObjectDataInput;
import com.hazelcast.logging.ILogger;
import com.hazelcast.table.impl.SelectByKeyOperation;
import com.hazelcast.table.impl.UpsertOperation;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;

import static com.hazelcast.internal.nio.IOUtil.compactOrClear;
import static com.hazelcast.spi.impl.reactor.Op.RUN_CODE_DONE;
import static com.hazelcast.spi.impl.reactor.Op.RUN_CODE_FOO;
import static com.hazelcast.spi.impl.reactor.OpCodes.TABLE_SELECT_BY_KEY;
import static com.hazelcast.spi.impl.reactor.OpCodes.TABLE_UPSERT;
import static java.nio.channels.SelectionKey.OP_READ;

class Reactor extends Thread {
    private final ReactorFrontEnd reactorService;
    private final Selector selector;
    private final ILogger logger;
    private final int port;
    private ServerSocketChannel serverSocketChannel;
    public final ConcurrentLinkedQueue taskQueue = new ConcurrentLinkedQueue();
    private final PacketIOHelper packetIOHelper = new PacketIOHelper();

    public Reactor(ReactorFrontEnd reactorService, Address thisAddress, int port) {
        super("Reactor:[" + thisAddress.getHost() + ":" + thisAddress.getPort() + "]:" + port);
        this.reactorService = reactorService;
        this.logger = reactorService.logger;
        this.selector = SelectorOptimizer.newSelector(reactorService.logger);
        this.port = port;
    }

    public void wakeup() {
        if (Thread.currentThread() != this) {
            System.out.println("wakeup");
            selector.wakeup();
        }
    }

    public void enqueue(Request request) {
        taskQueue.add(request);
        wakeup();
    }

    public void enqueue(Packet request) {
        taskQueue.add(request);
        wakeup();
    }

    public Future<Channel> enqueue(SocketAddress address) {
        logger.info("Connect to " + address);

        ConnectRequest connectRequest = new ConnectRequest();
        connectRequest.address = address;
        connectRequest.future = new CompletableFuture<>();
        taskQueue.add(connectRequest);

        wakeup();

        return connectRequest.future;
    }

    static class ConnectRequest {
        SocketAddress address;
        CompletableFuture<Channel> future;
    }

    public void run() {
        try {
            if (bind()) {
                loop();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private boolean bind() {
        InetSocketAddress address = null;
        try {
            InetAddress host = InetAddress.getLocalHost();
            address = new InetSocketAddress(host, port);
            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.bind(address);
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
            logger.info("ServerSocket listening at " + address);
            return true;
        } catch (IOException e) {
            logger.severe("Could not bind to " + address);
        }
        return false;
    }

    private void loop() throws Exception {
        while (!reactorService.shuttingdown) {

            int keyCount = selector.select();

            Thread.sleep(1000);

            System.out.println(this + " selectionCount:" + keyCount);

            //if (keyCount > 0) {
            processSelectionKeys();
            //}

            processTasks();
        }
    }

    private void processSelectionKeys() throws IOException {
        Set<SelectionKey> selectionKeys = selector.selectedKeys();
        Iterator<SelectionKey> it = selectionKeys.iterator();
        while (it.hasNext()) {
            SelectionKey key = it.next();
            it.remove();

            System.out.println(this + " selectionKey:" + key);

            System.out.println(this + " isWritable: " + key.isWritable());
            System.out.println(this + " isReadable: " + key.isReadable());
            System.out.println(this + " isAcceptable: " + key.isAcceptable());
            System.out.println(this + " isConnectable: " + key.isConnectable());


//            if (key.isValid() && key.isConnectable()) {
//                SocketChannel socketChannel = serverSocketChannel.accept();
//                socketChannel.configureBlocking(false);
//                key.attach(newChannel(socketChannel));
//                socketChannel.register(selector, OP_READ);
//                logger.info("Connection Accepted: " + socketChannel.getLocalAddress());
//            }

            if (key.isValid() && key.isAcceptable()) {
                SocketChannel socketChannel = serverSocketChannel.accept();
                socketChannel.configureBlocking(false);
                SelectionKey selectionKey = socketChannel.register(selector, OP_READ);
                selectionKey.attach(newChannel(socketChannel));

                logger.info("Connection Accepted: " + socketChannel.getLocalAddress());
            }

            if (key.isValid() && key.isReadable()) {
                SocketChannel socketChannel = (SocketChannel) key.channel();
                Channel channel = (Channel) key.attachment();
                ByteBuffer readBuf = channel.readBuffer;
                int bytesRead = socketChannel.read(readBuf);
                System.out.println(this + " bytes read: " + bytesRead);
                if (bytesRead <= 0) {
                    socketChannel.close();
                    key.cancel();
                    break;
                }
                readBuf.flip();
                process(readBuf);
                compactOrClear(readBuf);
            }

            if (!key.isValid()) {
                System.out.println("sk not valid");
                key.cancel();
            }
        }
    }

    private Channel newChannel(SocketChannel socketChannel) {
        System.out.println(this + " newChannel: " + socketChannel);

        Channel channel = new Channel();
        channel.reactor = this;
        channel.readBuffer = ByteBuffer.allocate(256 * 1024);
        channel.socketChannel = socketChannel;
        return channel;
    }

    private void processTasks() {
        for (; ; ) {
            Object item = taskQueue.poll();
            if (item == null) {
                return;
            }

            if (item instanceof Channel) {
                process((Channel) item);
            } else if (item instanceof ConnectRequest) {
                process((ConnectRequest) item);
            } else if (item instanceof Packet) {
                process((Packet) item);
            } else if (item instanceof Op) {
                proces((Op) item);
            } else if (item instanceof Request) {
                proces((Request) item);
            } else {
                throw new RuntimeException("Unregonized type:" + item.getClass());
            }
        }
    }

    private void process(Channel channel) {
        System.out.println("Processing channel");
        try {
            for (; ; ) {
                ByteBuffer buffer = channel.next();
                if (buffer == null) {
                    break;
                }

                int written = channel.socketChannel.write(buffer);
                System.out.println("written:" + written);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void process(ConnectRequest connectRequest) {
        try {
            SocketAddress address = connectRequest.address;
            System.out.println("makeConnections address:" + address);

            SocketChannel socketChannel = SocketChannel.open();
            // todo: call is blocking
            socketChannel.connect(address);
            socketChannel.configureBlocking(false);
            SelectionKey key = socketChannel.register(selector, OP_READ);

            Channel channel = newChannel(socketChannel);
            key.attach(channel);

            logger.info("Socket listening at " + address);
            connectRequest.future.complete(channel);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void process(ByteBuffer buffer) {
        for (; ; ) {
            Packet packet = packetIOHelper.readFrom(buffer);
            System.out.println(this + " read packet: " + packet);
            if (packet == null) {
                return;
            }

            process(packet);
        }
    }

    private void process(Packet packet) {
        System.out.println(this + " process packet: " + packet);
        try {
            byte[] bytes = packet.toByteArray();
            byte opcode = bytes[Packet.DATA_OFFSET];
            Op op = allocateOp(opcode);
            op.in.init(packet.toByteArray(), Packet.DATA_OFFSET + 1);
            proces(op);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void proces(Request request) {
        System.out.println("request: " + request);
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
        op.in = new ByteArrayObjectDataInput(null, (InternalSerializationService) reactorService.ss, ByteOrder.BIG_ENDIAN);
        op.managers = reactorService.managers;
        return op;
    }

    private void free(Op op) {
        op.cleanup();

        //we should return it to the pool.
    }
}
