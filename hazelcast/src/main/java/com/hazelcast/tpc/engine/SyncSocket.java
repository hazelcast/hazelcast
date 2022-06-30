package com.hazelcast.tpc.engine;

import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.tpc.engine.frame.Frame;

import java.io.Closeable;
import java.io.UncheckedIOException;
import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;


/**
 * The SyncSocket is blocking; so therefor should not be used inside an {@link Eventloop}.
 *
 * Most methods in this class are not thread-safe.
 */
public abstract class SyncSocket implements Closeable {

    public final ConcurrentMap context = new ConcurrentHashMap();

    protected final ILogger logger = Logger.getLogger(getClass());
    protected final AtomicBoolean closed = new AtomicBoolean();

    protected volatile SocketAddress remoteAddress;
    protected volatile SocketAddress localAddress;

    protected final SwCounter framesWritten = newSwCounter();

    protected final SwCounter framesRead = newSwCounter();

    protected final SwCounter bytesRead = newSwCounter();

    protected final SwCounter bytesWritten = newSwCounter();

    public final long framesWritten() {
        return framesWritten.get();
    }

    public final long bytesRead() {
        return bytesRead.get();
    }

    public final long bytesWritten() {
        return bytesWritten.get();
    }

    public final long framesRead() {
        return framesRead.get();
    }

    /**
     * Returns the remote address.
     *
     * If the SyncSocket isn't connected, null is returned.
     *
     * This method is thread-safe.
     *
     * @return the remote address.
     */
    public final SocketAddress remoteAddress() {
        return remoteAddress;
    }

    /**
     * Returns the local address.
     *
     * If the SyncSocket isn't connected, null is returned.
     *
     * This method is thread-safe.
     *
     * @return the local address.
     */
    public final SocketAddress localAddress() {
        return localAddress;
    }

    public abstract void soLinger(int soLinger);

    public abstract int soLinger();

    public abstract void keepAlive(boolean keepAlive);

    public abstract boolean isKeepAlive();

    public abstract void tcpNoDelay(boolean tcpNoDelay);

    public abstract boolean isTcpNoDelay();

    public abstract void receiveBufferSize(int size);

    public abstract int receiveBufferSize();

    public abstract void sendBufferSize(int size);

    public abstract int sendBufferSize();

    /**
     * Reads a single Frame from this SyncSocket and will block if there is no frame.
     *
     * This method is not thread-safe.
     *
     * @return the read Frame.
     * @throws java.io.UncheckedIOException if a problem happened while reading the frame.
     */
    public abstract Frame read();

    /**
     * Tries to read a single Frame from this SyncSocket.
     *
     * This method is not thread-safe.
     *
     * @return the read frame or null if not enough data was available to read a full frame.
     * @throws java.io.UncheckedIOException if a problem happened while reading the frame.
     */
    public abstract Frame tryRead();

    /**
     * Writes any scheduled frames are flushed to the socket. THis call blocks until any
     * scheduled frame have been written to the socket.
     *
     * This method is not thread-safe.
     */
    public abstract void flush();

    /**
     * Writes a frame to the SyncSocket. The frame isn't actually written; it is just
     * buffered.
     *
     * This call can be used to buffer a series of request and then call
     * {@link #flush()}.
     *
     * This method is not thread-safe.
     *
     * There is no guarantee that frame is actually going to be received by the caller if
     * the SyncSocket has accepted the frame. E.g. when the connection closes.
     *
     * @param frame the frame to write.
     * @return true if the frame was accepted, false if there was an overload.
     */
    public abstract boolean write(Frame frame);

    /**
     * Writes a frame and writes it to the socket.
     *
     * This is the same as calling {@link #write(Frame)} followed by a {@link #flush()}.
     *
     * There is no guarantee that frame is actually going to be received by the caller if
     * the SyncSocket has accepted the frame. E.g. when the connection closes.
     *
     * This method is thread-safe.
     *
     * If there was no space, a {@link #flush()} is still triggered.
     *
     * @param frame the frame to write.
     * @return true if the frame was accepted, false if there was an overload.
     */
    public abstract boolean writeAndFlush(Frame frame);

    /**
     * Connects synchronously to some address.
     *
     * @param address the address to connect to.
     * @throws UncheckedIOException if the connection could not be established.
     */
    public abstract void connect(SocketAddress address);

    /**
     * Closes this {@link SyncSocket}.
     *
     * This method is thread-safe.
     *
     * If the AsyncSocket is already closed, the call is ignored.
     */
    public abstract void close();

    /**
     * Checks if this SyncSocket is closed.
     *
     * This method is thread-safe.
     *
     * @return true if closed, false otherwise.
     */
    public final boolean isClosed() {
        return closed.get();
    }

    @Override
    public final String toString() {
        return getClass().getSimpleName() + "[" + localAddress + "->" + remoteAddress + "]";
    }
}

