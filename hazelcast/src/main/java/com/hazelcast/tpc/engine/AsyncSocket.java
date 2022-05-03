package com.hazelcast.tpc.engine;

import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.tpc.engine.frame.Frame;

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;


/**
 * Thoughts:
 *
 * Back pressure.
 *
 * - every queue/buffer that doesn't have a capacity constraint can lead overload problems and
 * back pressure is needed.
 *
 *
 * Reliable communication.
 *
 * Add sessions. So a frame gets send as part of some session. Each frame in a session is numbered. So gaps and
 * out of order messages can be detected.
 *
 * How would retransmission for a certain session work? On the sending side it is easy to store the frames
 * in a log (buffer) per session.
 *
 * But the other side needs to tell what has been received, so that logged messages can be dropped.
 *
 * Currently, the NioChannel itself will be dropped when the connection drops. A more durable mechanism is required.
 *
 * Also the
 */
public abstract class AsyncSocket implements Closeable {
    protected final ILogger logger = Logger.getLogger(getClass());
    protected final AtomicBoolean closed = new AtomicBoolean();

    protected volatile SocketAddress remoteAddress;
    protected volatile SocketAddress localAddress;

    public final SwCounter framesWritten = newSwCounter();

    public final SwCounter bytesRead = newSwCounter();

    public final SwCounter bytesWritten = newSwCounter();

    public final SwCounter framesRead = newSwCounter();

    public final SwCounter handleWriteCnt = newSwCounter();

    public final SwCounter readEvents = newSwCounter();

    protected final EventloopTask writeDirtySocket = () -> {
        try {
            handleWriteReady();
        }catch (IOException e){
            handleException(e);
        }
    };

    public final SocketAddress getRemoteAddress() {
        return remoteAddress;
    }

    public final SocketAddress getLocalAddress() {
        return localAddress;
    }

    public abstract void setTcpNoDelay(boolean tcpNoDelay);

    public abstract boolean isTcpNoDelay();

    public abstract void setReceiveBufferSize(int size);

    public abstract int getReceiveBufferSize();

    public abstract void setSendBufferSize(int size);

    public abstract int getSendBufferSize();

    public abstract void setReadHandler(ReadHandler readHandler);

    public abstract void activate(Eventloop eventloop);

    public abstract void flush();

    public abstract void write(Frame frame);

    public abstract void writeAll(Collection<Frame> frames);

    public abstract void writeAndFlush(Frame frame);

    /**
     * Should only be called from within the Eventloop.
     */
    public abstract void unsafeWriteAndFlush(Frame frame);

    public abstract void handleWriteReady() throws IOException;

    public abstract void handleException(Exception e);

    public abstract CompletableFuture<AsyncSocket> connect(SocketAddress address);

    public abstract void close();

    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + localAddress + "->" + remoteAddress + "]";
    }
}
