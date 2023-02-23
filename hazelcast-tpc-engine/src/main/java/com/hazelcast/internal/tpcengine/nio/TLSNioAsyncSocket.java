package com.hazelcast.internal.tpcengine.nio;

import com.hazelcast.internal.tpcengine.net.AsyncSocket;
import com.hazelcast.internal.tpcengine.net.AsyncSocketOptions;
import com.hazelcast.internal.tpcengine.net.AsyncSocketReader;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import com.hazelcast.internal.tpcengine.util.BufferUtil;
import com.hazelcast.internal.tpcengine.util.CircularQueue;
import org.jctools.queues.MpmcArrayQueue;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.tpcengine.util.BufferUtil.compactOrClear;
import static com.hazelcast.internal.tpcengine.util.BufferUtil.upcast;
import static com.hazelcast.internal.tpcengine.util.CloseUtil.closeQuietly;
import static com.hazelcast.internal.tpcengine.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;
import static java.nio.channels.SelectionKey.OP_CONNECT;
import static java.nio.channels.SelectionKey.OP_READ;
import static java.nio.channels.SelectionKey.OP_WRITE;


/**
 * https://docs.oracle.com/javase/10/security/sample-code-illustrating-use-sslengine.htm#JSSEC-GUID-EDE915F0-427B-48C7-918F-23C44384B862
 * <p>
 * https://github.com/alkarn/sslengine.example/blob/master/src/main/java/alkarn/github/io/sslengine/example/NioSslServer.java
 */
public class TLSNioAsyncSocket extends AsyncSocket {

    private final NioAsyncSocketOptions options;
    private final AtomicReference<Thread> flushThread = new AtomicReference<>();
    private final MpmcArrayQueue<IOBuffer> writeQueue;
    private final Handler handler;
    private final SocketChannel socketChannel;
    private final NioReactor reactor;
    private final Thread eventloopThread;
    private final SelectionKey key;
    private final boolean regularSchedule;
    private final boolean writeThrough;
    private final AsyncSocketReader reader;
    private final CircularQueue localTaskQueue;

    // only accessed from eventloop thread
    private boolean started;
    // only accessed from eventloop thread
    private boolean connecting;
    private CompletableFuture<Void> connectFuture;

    TLSNioAsyncSocket(NioAsyncSocketBuilder builder) {
        super(builder.clientSide);

        assert Thread.currentThread() == builder.reactor.eventloopThread();

        try {
            this.reactor = builder.reactor;
            this.localTaskQueue = builder.reactor.eventloop().localTaskQueue;
            this.options = builder.options;
            this.eventloopThread = reactor.eventloopThread();
            this.socketChannel = builder.socketChannel;
            if (!clientSide) {
                this.localAddress = socketChannel.getLocalAddress();
                this.remoteAddress = socketChannel.getRemoteAddress();
            }
            this.writeThrough = builder.writeThrough;
            this.regularSchedule = builder.regularSchedule;
            this.writeQueue = new MpmcArrayQueue<>(builder.writeQueueCapacity);
            this.handler = new Handler(builder);
            this.key = socketChannel.register(reactor.selector, 0, handler);
            this.reader = builder.reader;
            this.flushThread.set(eventloopThread);
            reader.init(this);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public AsyncSocketOptions options() {
        return options;
    }

    @Override
    public NioReactor reactor() {
        return reactor;
    }

    @Override
    public void setReadable(boolean readable) {
        if (Thread.currentThread() == eventloopThread) {
            setReadable0(readable);
        } else {
            CompletableFuture future = new CompletableFuture();
            reactor.execute(() -> {
                try {
                    setReadable0(readable);
                    future.complete(null);
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                    throw sneakyThrow(t);
                }
            });

            future.join();
        }
    }

    private void setReadable0(boolean readable) {
        if (readable) {
            key.interestOps(key.interestOps() | OP_READ);
        } else {
            key.interestOps(key.interestOps() & ~OP_READ);
        }
    }

    @Override
    public boolean isReadable() {
        if (Thread.currentThread() == eventloopThread) {
            return isReadable0();
        } else {
            CompletableFuture<Boolean> future = new CompletableFuture<>();
            reactor.execute(() -> {
                try {
                    future.complete(isReadable0());
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                    throw sneakyThrow(t);
                }
            });

            return future.join();
        }
    }

    private boolean isReadable0() {
        return (key.interestOps() & OP_READ) != 0;
    }

    @Override
    public void start() {
        if (Thread.currentThread() == reactor.eventloopThread()) {
            start0();
        } else {
            CompletableFuture future = new CompletableFuture();
            reactor.execute(() -> {
                try {
                    start0();
                    future.complete(null);
                } catch (Throwable e) {
                    future.completeExceptionally(e);
                    throw sneakyThrow(e);
                }
            });
            future.join();
        }
    }

    private void start0() {
        if (started) {
            throw new IllegalStateException(this + " is already started");
        }
        started = true;

        assert flushThread.get() == reactor.eventloopThread();

        if (!clientSide) {
            // on the server side we immediately start reading.
            key.interestOps(key.interestOps() | OP_READ);

            // we immediately need to schedule the handler so that the TLS handshake can start
            localTaskQueue.add(handler);
        }
    }

    @Override
    public CompletableFuture<Void> connect(SocketAddress address) {
        checkNotNull(address, "address");

        if (logger.isInfoEnabled()) {
            logger.info("Connecting to address:" + address);
        }

        CompletableFuture<Void> future = new CompletableFuture<>();
        if (Thread.currentThread() == eventloopThread) {
            connect0(address, future);
        } else {
            reactor.execute(() -> connect0(address, future));
        }

        return future;
    }

    private void connect0(SocketAddress address, CompletableFuture<Void> future) {
        try {
            if (!started) {
                throw new IllegalStateException(this + " can't connect when socket not yet started");
            }

            if (connecting) {
                throw new IllegalStateException(this + " is already trying to connect");
            }

            assert flushThread.get() == reactor.eventloopThread();

            connecting = true;
            connectFuture = future;
            key.interestOps(key.interestOps() | OP_CONNECT);
            socketChannel.connect(address);
        } catch (Throwable e) {
            future.completeExceptionally(e);
            throw sneakyThrow(e);
        }
    }

    @Override
    public void flush() {
        Thread currentThread = Thread.currentThread();
        if (flushThread.compareAndSet(null, currentThread)) {
            if (currentThread == eventloopThread) {
                localTaskQueue.add(handler);
            } else if (writeThrough) {
                handler.run();
            } else if (regularSchedule) {
                // todo: return value
                reactor.offer(handler);
            } else {
                key.interestOps(key.interestOps() | OP_WRITE);
                // we need to call the select wakeup because the interest set will only take
                // effect after a select operation.
                reactor.wakeup();
            }
        }
    }

    private void resetFlushed() {
        flushThread.set(null);

        if (!writeQueue.isEmpty()) {
            if (flushThread.compareAndSet(null, Thread.currentThread())) {
                reactor.offer(handler);
            }
        }
    }

    @Override
    public boolean write(IOBuffer buf) {
        return writeQueue.add(buf);
    }

    @Override
    public boolean writeAll(Collection<IOBuffer> bufs) {
        return writeQueue.addAll(bufs);
    }

    @Override
    public boolean writeAndFlush(IOBuffer buf) {
        boolean result = write(buf);
        flush();
        return result;
    }

    @Override
    public boolean unsafeWriteAndFlush(IOBuffer buf) {
        Thread currentFlushThread = flushThread.get();
        Thread currentThread = Thread.currentThread();

        assert currentThread == eventloopThread;

        boolean result;
        if (currentFlushThread == null) {
            if (flushThread.compareAndSet(null, currentThread)) {
                localTaskQueue.add(handler);
                result = writeQueue.offer(buf);
            } else {
                result = writeQueue.offer(buf);
            }
        } else if (currentFlushThread == eventloopThread) {
            result = writeQueue.offer(buf);
        } else {
            result = writeQueue.offer(buf);
            flush();
        }
        return result;
    }

    @Override
    protected void close0() throws IOException {
        closeQuietly(socketChannel);
        key.cancel();
        super.close0();
    }

    private class Handler implements NioHandler, Runnable {
        final ByteBuffer receiveBuffer;
        final ByteBuffer sendBuffer;
        private final SSLEngine sslEngine;
        private final ByteBuffer appBuffer;
        private final ByteBuffer emptyBuffer = ByteBuffer.allocate(0);
        private boolean handshakeComplete;

        private Handler(NioAsyncSocketBuilder builder) throws SocketException {
            int receiveBufferSize = builder.socketChannel.socket().getReceiveBufferSize();
            this.receiveBuffer = builder.receiveBufferIsDirect
                    ? ByteBuffer.allocateDirect(receiveBufferSize)
                    : ByteBuffer.allocate(receiveBufferSize);
            this.sslEngine = builder.sslEngineFactory.create(clientSide, null);
            this.appBuffer = ByteBuffer.allocate(64 * 1024);
            this.sendBuffer = ByteBuffer.allocate(64 * 1024);

            try {
                sslEngine.beginHandshake();
            } catch (SSLException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void run() {
            try {
                handleWrite();
            } catch (Throwable e) {
                close(null, e);
                throw sneakyThrow(e);
            }
        }

        @Override
        public void close(String reason, Throwable cause) {
            if (cause instanceof EOFException) {
                // The stacktrace of an EOFException isn't important. It just means that the
                // Exception is closed by the remote side.
                TLSNioAsyncSocket.this.close(reason != null ? reason : cause.getMessage(), null);
            } else {
                TLSNioAsyncSocket.this.close(reason, cause);
            }
        }

        @Override
        public void handle() throws IOException {
            if (!key.isValid()) {
                throw new CancelledKeyException();
            }

            int readyOps = key.readyOps();

            if ((readyOps & OP_READ) != 0) {
                handleRead();
            }

            if ((readyOps & OP_WRITE) != 0) {
                handleWrite();
            }

            if ((readyOps & OP_CONNECT) != 0) {
                handleConnect();
            }
        }

        private void handleRead() throws IOException {
            System.out.println(TLSNioAsyncSocket.this + " handleRead " + BufferUtil.toDebugString("receiveBuffer", receiveBuffer));
            metrics.incReadEvents();

            if (handshakeComplete) {
                metrics.incReadEvents();

                int read = socketChannel.read(receiveBuffer);
//                System.out.println(TLSNioAsyncSocket.this + " bytes read: " + read);

                if (read == -1) {
                    throw new EOFException("Remote socket closed!");
                }

                metrics.incBytesRead(read);
                upcast(receiveBuffer).flip();

                SSLEngineResult unwrapResult = sslEngine.unwrap(receiveBuffer, appBuffer);
                compactOrClear(receiveBuffer);
                appBuffer.flip();

                System.out.println(TLSNioAsyncSocket.this + " handleRead: unwrapStatus " + unwrapResult);
                System.out.println(TLSNioAsyncSocket.this + " scheduled: " + (flushThread.get() != null));

                switch (unwrapResult.getStatus()) {
                    case OK:
                        reader.onRead(appBuffer);
                        compactOrClear(appBuffer);
                        break;
                    case CLOSED:
                        throw new RuntimeException();
                    case BUFFER_OVERFLOW:
                        System.out.println(TLSNioAsyncSocket.this + " handleRead appBuffer " + BufferUtil.toDebugString(appBuffer));
                        throw new RuntimeException();
                    case BUFFER_UNDERFLOW:
                        // not enough data available.
                        compactOrClear(appBuffer);
                        break;
                    default:
                        throw new IllegalStateException("Unknown unwrapResult:" + unwrapResult);
                }
            } else {
                doHandshake();
                if (handshakeComplete) {
                    handleRead();
                }
            }
        }

        public void handleWrite() throws IOException {
            // typically this method is called with the flushThread being set.
            // but in case of cancellation of the key, this method is also
            // called without the flushThread being set.
            // So we can't do an assert flushThread!=null.

            metrics.incWriteEvents();

            if (!handshakeComplete) {
                doHandshake();
                if (handshakeComplete) {
                    handleWrite();
                }
            } else {
                assert flushThread.get() != null;

                System.out.println(this + " ------------------------------- handle normal write");

                boolean stop = false;
                while (!stop) {
                    IOBuffer buffer = writeQueue.poll();
                    if (buffer == null) {
                        break;
                    }

                    System.out.println("------------ writing user stuff");
                    SSLEngineResult wrapStatus = sslEngine.wrap(buffer.byteBuffer(), sendBuffer);
                    System.out.println(TLSNioAsyncSocket.this + " handleWrite: wrapStatus " + wrapStatus);
                    switch (wrapStatus.getStatus()) {
                        case OK:
                            stop = true;
                            break;
                        case CLOSED:
                            break;
                        case BUFFER_OVERFLOW:
                            break;
                        case BUFFER_UNDERFLOW:
                            break;
                        default:
                            throw new IllegalStateException("Unknown wrapStatus:" + wrapStatus);
                    }
                }

                sendBuffer.flip();
                long written = socketChannel.write(sendBuffer);
                compactOrClear(sendBuffer);

                metrics.incBytesWritten(written);
//                System.out.println(TLSNioAsyncSocket.this + " bytes written:" + written);

                if (true) {
                    // everything got written
                    int interestOps = key.interestOps();

                    // clear the OP_WRITE flag if it was set
                    if ((interestOps & OP_WRITE) != 0) {
                        key.interestOps(interestOps & ~OP_WRITE);
                    }

                    resetFlushed();
                } else {
                    // We need to register for the OP_WRITE because not everything got written
                    key.interestOps(key.interestOps() | OP_WRITE);
                }
            }
        }

        private void doHandshake() throws IOException {
            while (true) {
                SSLEngineResult.HandshakeStatus handshakeStatus = sslEngine.getHandshakeStatus();
                System.out.println(TLSNioAsyncSocket.this + " handshakeStatus " + handshakeStatus);
//                try {
//                    Thread.sleep(100);
//                } catch (InterruptedException e) {
//                    throw new RuntimeException(e);
//                }

                switch (handshakeStatus) {
                    case FINISHED:
                        throw new IllegalStateException("This result can never be returned by sslEngine.getHandhshake status; only through wrap/unwrap");
                    case NEED_TASK:
                        // todo: this is blocking, no good.
                        Runnable runnable = sslEngine.getDelegatedTask();
                        while (runnable != null) {
                            System.out.println(TLSNioAsyncSocket.this + " handshakeStatus processing " + runnable);
                            runnable.run();
                            runnable = sslEngine.getDelegatedTask();
                        }
                        break;
                    case NEED_WRAP: {
                        System.out.println(BufferUtil.toDebugString(sendBuffer));
                        SSLEngineResult wrapResult = sslEngine.wrap(emptyBuffer, sendBuffer);

                        System.out.println(TLSNioAsyncSocket.this + " handshake wrapResult " + wrapResult);
                        switch (wrapResult.getStatus()) {
                            case BUFFER_UNDERFLOW:
                                throw new RuntimeException("Buffer underflow");
                            case BUFFER_OVERFLOW:
                                throw new RuntimeException("Buffer overflow");
                            case CLOSED:
                                throw new RemoteException("Closed");
                            case OK:
                                sendBuffer.flip();
                                long written = socketChannel.write(sendBuffer);

                                metrics.incBytesWritten(written);
                                System.out.println(TLSNioAsyncSocket.this + " handshakeStatus bytes written:" + written);
                                compactOrClear(sendBuffer);

                                if (wrapResult.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.FINISHED) {
                                    handshakeComplete = true;
                                    System.out.println(TLSNioAsyncSocket.this + " handshake complete!!");
                                    resetFlushed();
                                }

                                return;
                            default:
                                throw new RuntimeException("Unknown wrapResult:" + wrapResult);
                        }
                    }
                    case NEED_UNWRAP: {
                        System.out.println(TLSNioAsyncSocket.this + " " + BufferUtil.toDebugString(receiveBuffer));
                        int read = socketChannel.read(receiveBuffer);
                        System.out.println(TLSNioAsyncSocket.this + " handshake bytes read: " + read);

                        if (read == -1) {
                            throw new EOFException("Remote socket closed!");
                        }

                        receiveBuffer.flip();
                        if (receiveBuffer.remaining() == 0) {
                            receiveBuffer.clear();
                            System.out.println(TLSNioAsyncSocket.this + " handshake no data for NEED_UNWRAP");
                            return;
                        }

                        SSLEngineResult wrapResult = sslEngine.unwrap(receiveBuffer, emptyBuffer);

                        compactOrClear(receiveBuffer);
                        System.out.println(TLSNioAsyncSocket.this + " handshake wrapResult " + wrapResult);

                        switch (wrapResult.getStatus()) {
                            case BUFFER_UNDERFLOW:
                                //todo: we should do more reading at some point in the future.
                                throw new RuntimeException("Buffer underflow");
                            case BUFFER_OVERFLOW:
                                throw new RuntimeException("Buffer overflow");
                            case CLOSED:
                                throw new RemoteException("Closed");
                            case OK:
                                if (wrapResult.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.FINISHED) {
                                    System.out.println(TLSNioAsyncSocket.this + "handshake complete!!");
                                    handshakeComplete = true;
                                    resetFlushed();
                                    return;
                                }

                                break;
                            default:
                                throw new RuntimeException("Unknown wrapResult:" + wrapResult);
                        }
                        break;
                    }
                    case NOT_HANDSHAKING:
                        throw new IOException("Failed to complete the SSL/TLS handshake");
                    default:
                        throw new IllegalStateException("Unknown handshakeStatus:" + handshakeStatus);
                }
            }
        }

        // Is called when side of the socket that initiates the connect
        // gets the event that the connection is completed.
        private void handleConnect() {
            try {
                if (!socketChannel.finishConnect()) {
                    throw new IllegalStateException();
                }
                onConnectFinished();
            } catch (Throwable e) {
                if (connectFuture != null) {
                    connectFuture.completeExceptionally(e);
                }
                throw sneakyThrow(e);
            } finally {
                connectFuture = null;
            }
        }

        private void onConnectFinished() throws IOException {
            assert connecting;
            assert flushThread.get() == reactor.eventloopThread();

            remoteAddress = socketChannel.getRemoteAddress();
            localAddress = socketChannel.getLocalAddress();
            if (logger.isInfoEnabled()) {
                logger.info("Connection established " + TLSNioAsyncSocket.this);
            }

            key.interestOps(key.interestOps() | OP_READ);
            connectFuture.complete(null);
            connectFuture = null;

            // we immediately need to schedule the handler so that the TLS handshake can start
            localTaskQueue.add(this);
        }
    }
}
