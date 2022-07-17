package com.hazelcast.tpc.engine.nio;

import com.hazelcast.tpc.engine.SyncSocket;
import com.hazelcast.tpc.engine.iobuffer.IOBuffer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;


import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static java.net.StandardSocketOptions.SO_KEEPALIVE;
import static java.net.StandardSocketOptions.SO_LINGER;
import static java.net.StandardSocketOptions.SO_RCVBUF;
import static java.net.StandardSocketOptions.SO_SNDBUF;
import static java.net.StandardSocketOptions.TCP_NODELAY;

public final class NioSyncSocket extends SyncSocket {

    private final SocketChannel socketChannel;
    private final IOVector ioVector = new IOVector();
    private ByteBuffer receiveBuffer;
    private NioSyncReadHandler readHandler;

    public static NioSyncSocket open() {
        return new NioSyncSocket();
    }

    private NioSyncSocket() {
        try {
            this.socketChannel = SocketChannel.open();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void readHandler(NioSyncReadHandler readHandler) {
        this.readHandler = checkNotNull(readHandler, "readHandler can't be null");
    }

    public SocketChannel socketChannel() {
        return socketChannel;
    }

    @Override
    public void soLinger(int soLinger) {
        try {
            socketChannel.setOption(SO_LINGER, soLinger);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public int soLinger() {
        try {
            return socketChannel.getOption(SO_LINGER);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void keepAlive(boolean keepAlive) {
        try {
            socketChannel.setOption(SO_KEEPALIVE, keepAlive);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean isKeepAlive() {
        try {
            return socketChannel.getOption(SO_KEEPALIVE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean isTcpNoDelay() {
        try {
            return socketChannel.getOption(TCP_NODELAY);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void tcpNoDelay(boolean tcpNoDelay) {
        try {
            socketChannel.setOption(TCP_NODELAY, tcpNoDelay);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public int receiveBufferSize() {
        try {
            return socketChannel.getOption(SO_RCVBUF);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void receiveBufferSize(int size) {
        try {
            socketChannel.setOption(SO_RCVBUF, size);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public int sendBufferSize() {
        try {
            return socketChannel.getOption(SO_SNDBUF);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void sendBufferSize(int size) {
        try {
            socketChannel.setOption(SO_SNDBUF, size);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public IOBuffer read() {
        if (receiveBuffer == null) {
            receiveBuffer = ByteBuffer.allocateDirect(receiveBufferSize());
            receiveBuffer.flip();
        }

        try {
            for (; ; ) {
                IOBuffer buf = readHandler.decode(receiveBuffer);
                if (buf != null) {
                    return buf;
                }

                if (receiveBuffer.hasRemaining()) {
                    receiveBuffer.compact();
                } else {
                    receiveBuffer.clear();
                }

                int read = socketChannel.read(receiveBuffer);
                if (read == -1) {
                    close();
                    throw new IOException("Socket closed");
                } else {
                    bytesRead.inc(read);
                    receiveBuffer.flip();
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public IOBuffer tryRead() {
        if (receiveBuffer == null) {
            receiveBuffer = ByteBuffer.allocateDirect(receiveBufferSize());
            receiveBuffer.flip();
        }

        try {
            IOBuffer buf = readHandler.decode(receiveBuffer);
            if (buf != null) {
                return buf;
            }

            if (receiveBuffer.hasRemaining()) {
                receiveBuffer.compact();
            } else {
                receiveBuffer.clear();
            }

            int read = socketChannel.read(receiveBuffer);
            if (read == -1) {
                close();
                throw new IOException("Socket closed");
            } else {
                bytesRead.inc(read);
                receiveBuffer.flip();
            }

            return readHandler.decode(receiveBuffer);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void flush() {
        try {
            long written = ioVector.write(socketChannel);
            bytesWritten.inc(written);
            //System.out.println(this + " bytes written:" + written);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean write(IOBuffer buf) {
        checkNotNull(buf);
        return ioVector.add(buf);
    }

    @Override
    public boolean writeAndFlush(IOBuffer buf) {
        boolean result = write(buf);
        flush();
        return result;
    }

    @Override
    public void connect(SocketAddress address) {
        try {
            socketChannel.connect(address);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            closeResource(socketChannel);
        }
    }
}
