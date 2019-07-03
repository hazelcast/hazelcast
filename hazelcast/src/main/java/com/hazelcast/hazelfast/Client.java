package com.hazelcast.hazelfast;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static com.hazelcast.hazelfast.IOUtil.INT_AS_BYTES;
import static com.hazelcast.hazelfast.IOUtil.allocateByteBuffer;
import static com.hazelcast.hazelfast.IOUtil.compactOrClear;
import static com.hazelcast.hazelfast.IOUtil.setReceiveBufferSize;
import static com.hazelcast.hazelfast.IOUtil.setSendBufferSize;

public class Client {

    private final FramePool framePool;
    private InetSocketAddress address;
    private SocketChannel socketChannel;
    protected ByteBuffer sendBuf;
    private ByteBuffer receiveBuf;
    private final String hostname;
    private final int port;
    private final int receiveBufferSize;
    private final int sendBufferSize;
    private final boolean tcpNoDelay;
    private final boolean directBuffers;
    private final ByteArrayPool byteArrayPool;

    public Client(Context context) {
        this.hostname = context.hostname;
        this.port = context.port;
        this.receiveBufferSize = context.receiveBufferSize;
        this.sendBufferSize = context.sendBufferSize;
        this.tcpNoDelay = context.tcpNoDelay;
        this.directBuffers = context.directBuffers;
        this.byteArrayPool = new ByteArrayPool(context.objectPoolingEnabled);
        this.framePool = new FramePool(context.objectPoolingEnabled);

    }

    public String hostname() {
        return hostname;
    }

    public int receiveBufferSize() {
        return receiveBufferSize;
    }

    public int sendBufferSize() {
        return sendBufferSize;
    }

    public boolean tcpNoDelay() {
        return tcpNoDelay;
    }

    public boolean directBuffers() {
        return directBuffers;
    }

    public static void main(String[] args) throws Exception {
        Client client = new Client(new Context());
        client.start();

        client.dummyLoop();
        client.stop();
    }

    public void start()  {
       try {
           this.address = new InetSocketAddress(hostname, port);
           log("Connecting to Server "+address);

           sendBuf = allocateByteBuffer(directBuffers, receiveBufferSize);
           receiveBuf = allocateByteBuffer(directBuffers, sendBufferSize);
           socketChannel = SocketChannel.open(address);
           socketChannel.socket().setTcpNoDelay(tcpNoDelay);
           setReceiveBufferSize(socketChannel, receiveBufferSize);
           setSendBufferSize(socketChannel, sendBufferSize);
       }catch (Exception e){
           throw new RuntimeException(e);
       }
    }

    public void stop()  {
        try {
            socketChannel.close();
        } catch (IOException e) {
        }
    }

    public void dummyLoop() throws IOException {
        try {
            long startMs = System.currentTimeMillis();
            int count = 100000;
            for (int k = 0; k < count; k++) {
                System.out.println("Writing request1");
                writeAndFlush(new byte[100]);
                System.out.println("Writing request1");
                writeAndFlush(new byte[100]);
                System.out.println("Reading response1");
                readResponse();
                System.out.println("Reading response2");
                readResponse();
                System.out.println("k:" + k);
            }
            long durationMs = System.currentTimeMillis() - startMs;

            System.out.println("Duration:" + durationMs + " ms");
            System.out.println("Throughput:" + (1000f * count) / durationMs + " msg/second");
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    public void writeAndFlush(String message) throws IOException {
        writeAndFlush(message.getBytes());
    }

    public void writeAndFlush(byte[] message) throws IOException {
        write(message);
        flush();
    }

    public void flush() throws IOException {
        sendBuf.flip();
        int written = socketChannel.write(sendBuf);
        sendBuf.clear();
    }

    public void write(byte[] message) {
        sendBuf.putInt(message.length );
        sendBuf.put(message);
    }

    public void readResponse() throws IOException {
        Frame frame = null;
        int offset = 0;

        boolean skipRead = true;
        for (; ; ) {
            if (!skipRead) {
                int read = socketChannel.read(receiveBuf);
                if (read == -1) {
                    socketChannel.close();
                    throw new IOException("Socket Closed by remote");
                }
                //   System.out.println("read:" + read + " bytes");
            }

            skipRead = false;

            receiveBuf.flip();
            try {
                if (frame == null) {
                    if (receiveBuf.remaining() < INT_AS_BYTES) {
                        continue;
                    }

                    frame = framePool.takeFromPool();
                    frame.length = receiveBuf.getInt();
                    frame.bytes = byteArrayPool.takeFromPool(frame.length);
                }
                int needed = frame.length - offset;
                int length;
                boolean complete = false;
                if (receiveBuf.remaining() >= needed) {
                    length = needed;
                    complete = true;
                } else {
                    length = receiveBuf.remaining();
                }

                receiveBuf.get(frame.bytes, offset, length);
                if (complete) {
                    byteArrayPool.returnToPool(frame.bytes);
                    framePool.returnToPool(frame);
                    return;
                } else {
                    offset += length;
                }
            } finally {
                compactOrClear(receiveBuf);
            }
        }
    }

    private static void log(String str) {
        System.out.println(str);
    }

    public static class Context {
        private String hostname = "localhost";
        private int port = 1111;
        private int receiveBufferSize = 256 * 1024;
        private int sendBufferSize = 256 * 1024;
        private boolean tcpNoDelay = true;
        private boolean directBuffers = true;
        private boolean objectPoolingEnabled = true;

        public Context port(int port) {
            this.port = port;
            return this;
        }

        public Context hostname(String hostname) {
            this.hostname = hostname;
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

        public Context directBuffers(boolean directBuffers) {
            this.directBuffers = directBuffers;
            return this;
        }

        public Context objectPoolingEnabled(boolean objectPoolingEnabled) {
            this.objectPoolingEnabled = objectPoolingEnabled;
            return this;
        }
    }
}