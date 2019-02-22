package com.hazelcast.hazelfast;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.util.ClientProtocolBuffer;
import com.hazelcast.client.impl.protocol.util.SafeBuffer;
import com.hazelcast.logging.ILogger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketOption;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.hazelfast.IOUtil.INT_AS_BYTES;
import static com.hazelcast.hazelfast.IOUtil.allocateByteBuffer;
import static com.hazelcast.hazelfast.IOUtil.compactOrClear;
import static com.hazelcast.hazelfast.IOUtil.setReceiveBufferSize;
import static com.hazelcast.hazelfast.IOUtil.setSendBufferSize;
import static com.hazelcast.memory.MemoryUnit.KILOBYTES;
import static java.net.StandardSocketOptions.SO_RCVBUF;
import static java.net.StandardSocketOptions.SO_SNDBUF;

/**
 * A {@link Channel} that blocks while writing or reading from the underlying socket channel.
 *
 * The put operations write to the sendBuffer; but normally don't flush to the socket. An
 * explicit {@link #flush()} is required for that. But if there is more data than available
 * in the sendBuffer, it will flush so all data can be written.
 */
public class BlockingChannel implements Channel {

    private final FramePool framePool;
    private final Map<SocketOption, Object> socketOptions;
    private InetSocketAddress address;
    protected SocketChannel socketChannel;
    protected ByteBuffer sendBuf;
    private ByteBuffer receiveBuf;
    private final String hostname;
    private final int port;
    private final boolean directBuffers;
    private final ByteArrayPool byteArrayPool;
    protected final ILogger logger;
    protected final ConcurrentMap attributeMap = new ConcurrentHashMap();

    public BlockingChannel(Context context) {
        this.hostname = context.hostname;
        this.port = context.port;
        this.socketOptions = context.socketOptions;
        this.directBuffers = context.directBuffers;
        this.byteArrayPool = new ByteArrayPool(context.objectPoolingEnabled);
        this.framePool = new FramePool(context.objectPoolingEnabled);
        this.logger = context.logger;
    }

    /**
     * Returns the attribute map.
     *
     * Attribute map can be used to store data into a socket. For example to find the
     * Connection for a Channel, one can store the Connection in this channel using some
     * well known key.
     *
     * @return the attribute map.
     */
    public ConcurrentMap attributeMap() {
        return attributeMap;
    }

    /**
     * Connects this BlockingChannel to the host and configures the created socket.
     */
    public void connect() {
        if (socketChannel != null) {
            throw new IllegalStateException("Can't connect already connected BlockingChannel");
        }

        try {
            this.address = new InetSocketAddress(hostname, port);
            if (logger.isFineEnabled()) {
                logger.fine("Connecting to Server " + address);
            }
            this.socketChannel = SocketChannel.open(address);
            for (Map.Entry<SocketOption, Object> entry : socketOptions.entrySet()) {
                SocketOption option = entry.getKey();
                Object optionValue = entry.getValue();
                if (option.equals(SO_RCVBUF)) {
                    setReceiveBufferSize(socketChannel, (Integer) optionValue);
                    this.receiveBuf = allocateByteBuffer(directBuffers, socketChannel.socket().getReceiveBufferSize());
                } else if (option.equals(SO_SNDBUF)) {
                    setSendBufferSize(socketChannel, (Integer) optionValue);
                    this.sendBuf = allocateByteBuffer(directBuffers, socketChannel.socket().getSendBufferSize());
                } else {
                    socketChannel.setOption(option, optionValue);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isOpen() {
        return socketChannel.isOpen();
    }

    @Override
    public void close() throws IOException {
        if (logger.isFineEnabled()) {
            logger.fine("Closing channel " + toString());
        }
        socketChannel.close();
    }

    /**
     * Writes all data that has been accumulated to the socket.
     * <p>
     * This call is blocking; so it waits till all data in the sendBuffer has been written.
     *
     * @throws IOException
     */
    public void flush() {
        sendBuf.flip();
        try {
            int written = socketChannel.write(sendBuf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        sendBuf.clear();
    }

    protected void putInt(int value) {
        if (sendBuf.remaining() < INT_AS_BYTES) {
            flush();
        }

        sendBuf.putInt(value);
    }

    protected void put(byte[] bytes) {
        int bufferRemaining = sendBuf.remaining();
        int bytesRemaining = bytes.length;
        if (bufferRemaining >= bytesRemaining) {
            // enough space is available, so lets write it all.
            sendBuf.put(bytes);
        } else {
            // not enough space is available, so lets write it in batches.
            int offset = 0;
            for (; ; ) {
                int length = Math.min(sendBuf.remaining(), bytesRemaining);
                sendBuf.put(bytes, offset, length);
                offset += length;
                bytesRemaining -= length;
                if (bytesRemaining == 0) {
                    // all data got written, so we are finished. We are not obligated to
                    // flush the data that is bufferRemaining.
                    break;
                }

                // we need more space, so lets flush and try again
                flush();
            }
        }
    }

    @Override
    public String toString() {
        return socketChannel.toString();
    }

    // todo: this call is tied to
    // todo:
    public ClientMessage readResponse() throws IOException {
        Frame frame = null;
        int offset = 0;

        int totalBytesRead = 0;
        boolean skipRead = true;
        for (; ; ) {
            if (!skipRead) {
                //System.out.println("Before read:"+IOUtil.toDebugString("rcvBuffer", receiveBuf));
                int bytesRead = socketChannel.read(receiveBuf);
                // System.out.println("After read");
                totalBytesRead += bytesRead;
                // System.out.println("bytesRead from socket:" + bytesRead);
                //  System.out.println("total reads from socket:" + totalBytesRead);

                if (bytesRead == -1) {
                    socketChannel.close();
                    throw new IOException("Socket Closed by remote");
                }
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
                    //  System.out.println("Frame.length:" + frame.length);
                    //  System.out.println("Total bytes expected from socket:" + (frame.length + 4));
                    //  System.out.println("Missing:"+(frame.length+4-totalBytesRead));
                    frame.bytes = byteArrayPool.takeFromPool(frame.length);
                }
                int needed = frame.length - offset;
                int length;
                boolean complete = false;
                if (receiveBuf.remaining() >= needed) {
                    length = needed;
                    complete = true;
                    //  System.out.println("Complete = true");
                } else {
                    length = receiveBuf.remaining();
                }

                receiveBuf.get(frame.bytes, offset, length);
                if (complete) {
                    // could be pooled
                    ClientProtocolBuffer buffer = new SafeBuffer(frame.bytes);
                    ClientMessage message = ClientMessage.createForDecode(buffer, 0);

                    byteArrayPool.returnToPool(frame.bytes);
                    framePool.returnToPool(frame);
                    return message;
                } else {
                    offset += length;
                }
            } finally {
                compactOrClear(receiveBuf);
            }
        }
    }

    public static class Context {
        private String hostname = "localhost";
        private int port = 1111;
        private boolean directBuffers = true;
        private boolean objectPoolingEnabled = true;
        private ILogger logger;
        private Map<SocketOption, Object> socketOptions = new HashMap<>();

        public Context() {
            socketOptions.put(StandardSocketOptions.TCP_NODELAY, true);
            socketOptions.put(SO_SNDBUF, (int) KILOBYTES.toBytes(256));
            socketOptions.put(SO_RCVBUF, (int) KILOBYTES.toBytes(256));
        }

        public <T> Context socketOption(SocketOption<T> option, T value) {
            socketOptions.put(option, value);
            return this;
        }

        public Context logger(ILogger logger) {
            this.logger = logger;
            return this;
        }

        public Context port(int port) {
            this.port = port;
            return this;
        }

        public Context hostname(String hostname) {
            this.hostname = hostname;
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