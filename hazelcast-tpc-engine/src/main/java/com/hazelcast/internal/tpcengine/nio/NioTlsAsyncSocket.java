package com.hazelcast.internal.tpcengine.nio;

import com.hazelcast.internal.tpcengine.net.AsyncSocket;
import com.hazelcast.internal.tpcengine.net.AsyncSocketOptions;
import com.hazelcast.internal.tpcengine.net.AsyncSocketReader;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import com.hazelcast.internal.tpcengine.util.CircularQueue;
import org.jctools.queues.MpmcArrayQueue;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
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
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.FINISHED;


/**
 * A {@link AsyncSocket} that is Nio based (so uses a selector) and provides
 * TLS.
 * <p>
 * https://docs.oracle.com/javase/10/security/sample-code-illustrating-use-sslengine.htm#JSSEC-GUID-EDE915F0-427B-48C7-918F-23C44384B862
 * <p>
 * https://github.com/alkarn/sslengine.example/blob/master/src/main/java/alkarn/github/io/sslengine/example/NioSslServer.java
 */
public class NioTlsAsyncSocket extends AsyncSocket {

    private final NioAsyncSocketOptions options;
    private final AtomicReference<Thread> flushThread = new AtomicReference<>();
    private final MpmcArrayQueue<IOBuffer> writeQueue;
    private final TLsHandler handler;
    private final SocketChannel socketChannel;
    private final NioReactor reactor;
    private final Thread eventloopThread;
    private final SelectionKey key;
    private final IOVector ioVector = new IOVector();
    private final boolean regularSchedule;
    private final boolean writeThrough;
    private final AsyncSocketReader reader;
    private final CircularQueue localTaskQueue;

    // only accessed from eventloop thread
    private boolean started;
    // only accessed from eventloop thread
    private boolean connecting;
    private CompletableFuture<Void> connectFuture;

    NioTlsAsyncSocket(NioAsyncSocketBuilder builder) {
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
            this.handler = new TLsHandler(builder);
            this.key = socketChannel.register(reactor.selector, 0, handler);
            this.reader = builder.reader;
            // There is no need for the socket to be scheduled on startup because the
            // Handshake is already executing. Only when the handshake is done, the
            // flushthread will be unset.
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
            handler.run();
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
                if (ioVector.offer(buf)) {
                    result = true;
                } else {
                    result = writeQueue.offer(buf);
                }
            } else {
                result = writeQueue.offer(buf);
            }
        } else if (currentFlushThread == eventloopThread) {
            if (ioVector.offer(buf)) {
                result = true;
            } else {
                result = writeQueue.offer(buf);
            }
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

    private class TLsHandler implements NioHandler, Runnable {
        private final ByteBuffer receiveBuffer;
        private final ByteBuffer sendBuffer;
        private final SSLEngine sslEngine;
        private final boolean directBuffers;
        private SSLSession sslSession;
        private final ByteBuffer appBuffer;
        private final ByteBuffer emptyBuffer = ByteBuffer.allocate(0);
        private boolean handshakeInProgress = true;

        private TLsHandler(NioAsyncSocketBuilder builder) throws SocketException {
            this.directBuffers = builder.directBuffers;

            //todo: we need to pass the correct address.
            this.sslEngine = builder.sslEngineFactory.create(clientSide, null);

            int bufSize = 32 * 1024;
//            int receiveBufferSize = builder.socketChannel.socket().getReceiveBufferSize();
//            if(receiveBufferSize<bufSize){
//                receiveBufferSize = bufSize;
//            }
            this.receiveBuffer = directBuffers
                    ? ByteBuffer.allocateDirect(bufSize)
                    : ByteBuffer.allocate(bufSize);
            this.appBuffer = directBuffers
                    ? ByteBuffer.allocateDirect(bufSize)
                    : ByteBuffer.allocate(bufSize);

//            int sendBufferSize = builder.socketChannel.socket().getSendBufferSize();
//            if(sendBufferSize<bufSize){
//                sendBufferSize = bufSize;
//            }

            this.sendBuffer = directBuffers
                    ? ByteBuffer.allocateDirect(bufSize)
                    : ByteBuffer.allocate(bufSize);

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
                NioTlsAsyncSocket.this.close(reason != null ? reason : cause.getMessage(), null);
            } else {
                NioTlsAsyncSocket.this.close(reason, cause);
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
            metrics.incReadEvents();

            if (handshakeInProgress && !completeHandshake()) {
                return;
            }

            int read = socketChannel.read(receiveBuffer);
            //System.out.println(TLSNioAsyncSocket.this + " bytes read: " + read);
            if (read == -1) {
                throw new EOFException("Remote socket closed!");
            }

            metrics.incBytesRead(read);
            upcast(receiveBuffer).flip();

            boolean unwrapMore = true;
            do {
                // Reducing ByteBuffer-array litter.
                //
                // The following method leads to 2 ByteBuffer arrays being created
                //
                //      public SSLEngineResult unwrap(ByteBuffer src, ByteBuffer dst) throws SSLException {
                //
                // 1 for the receiveBuffer and 1 for the appBuffer. What happens internally is that src/dst
                // Are wrapped into singleton arrays e.g.
                //
                //      public SSLEngineResult unwrap(ByteBuffer src, ByteBuffer dst) throws SSLException {
                //          return unwrap(src, new ByteBuffer [] { dst }, 0, 1);
                //      }
                //
                // One slightly better version is this method.
                //
                //      public SSLEngineResult unwrap(ByteBuffer src, ByteBuffer [] dsts) throws SSLException {
                //
                // And manage the dst singleton array yourself. This way we can at least remove the array creation for
                // the dst. But removing the ByteBuffer array creation for source, is more complicated since the
                // SSLEngine doesn't expose the following method as part of the API:
                //
                //  public SSLEngineResult unwrap(
                //        ByteBuffer[] srcs, int srcsOffset, int srcsLength,
                //        ByteBuffer[] dsts, int dstsOffset, int dstsLength) throws SSLException {
                //
                // So we should just cast to the appropriate SSLEngine implementations if they are detected and
                // use the one that doesn't create the ByteBuffer array litter.
                // Do not try to perform this task because you see the comment here. Only perform this task
                // if you know what you are doing and are able to benchmark the before and after situation.
                SSLEngineResult unwrapResult = sslEngine.unwrap(receiveBuffer, appBuffer);
                //System.out.println(TLSNioAsyncSocket.this + " handleRead: unwrapResult " + unwrapResult.toString().replace("\n", " "));
                switch (unwrapResult.getStatus()) {
                    case OK:
                        if (!receiveBuffer.hasRemaining()) {
                            // no point in asking for another unwrap if there is no more data to unwrap.
                            unwrapMore = false;
                        }
                        break;
                    case CLOSED:
                        throw new EOFException("Socket closed!");
                    case BUFFER_OVERFLOW:
                        if (appBuffer.capacity() >= sslSession.getApplicationBufferSize()) {
                            // The appBuffer is large enough, but too much data was accumulated. So lets drain
                            // that to the read handler and go for another round of unwrapping
                            readHandlerOnRead();
                        } else {
                            // The appBuffer isn't large enough, so we need to grow it and try again.
                            //todo: we need to grow the appBuffer and try again
                            throw new RuntimeException();
                        }
                    case BUFFER_UNDERFLOW:
                        // not enough data available to decode, so wait for more data.
                        //compactOrClear(appBuffer);
                        //System.out.println(TLSNioAsyncSocket.this + " receiveBuffer " + BufferUtil.toDebugString(receiveBuffer));
                        //System.out.println(TLSNioAsyncSocket.this + " appBuffer " + BufferUtil.toDebugString(appBuffer));
                        unwrapMore = false;
                        break;
                    default:
                        throw new IllegalStateException("Unknown unwrapResult:" + unwrapResult);
                }
            } while (unwrapMore);

            readHandlerOnRead();

            compactOrClear(receiveBuffer);
        }

        private void readHandlerOnRead() {
            // the appBuffer was in writing mode, we first need to set to reading mode
            appBuffer.flip();
            // offer the appBuffer to the readHandler
            reader.onRead(appBuffer);
            // and set the appBuffer back into writing mode for more rounds of unwrapping
            compactOrClear(appBuffer);
        }

        public void handleWrite() throws IOException {
            // typically this method is called with the flushThread being set.
            // but in case of cancellation of the key, this method is also
            // called without the flushThread being set.
            // So we can't do an assert flushThread!=null.

            //System.out.println(this + " handleWrite");
            metrics.incWriteEvents();

            if (handshakeInProgress && !completeHandshake()) {
                return;
            }

            ioVector.populate(writeQueue);

            ByteBuffer[] srcs = ioVector.array();

            boolean wrapMore = true;
            do {
                // Litter is being created due to the implementation wrapping the sendBuffer into a singleton array
                // See the handleRead methods for a similar situation and how to deal with it.
                SSLEngineResult wrapResult = sslEngine.wrap(srcs, 0, ioVector.length(), sendBuffer);
                ioVector.compact(wrapResult.bytesConsumed());
                switch (wrapResult.getStatus()) {
                    case OK:
                        if (ioVector.isEmpty()) {
                            wrapMore = false;
                        }
                        break;
                    case CLOSED:
                        throw new EOFException("Remote socket closed!");
                    case BUFFER_OVERFLOW:
                        // there is not enough space in the sendBuffer, so lets submit what is there
                        // todo: what if the capacity of the buffer isn't sufficient?
                        wrapMore = false;
                        break;
                    case BUFFER_UNDERFLOW:
                        // can this happen?
                        throw new RuntimeException();
                    default:
                        throw new IllegalStateException("Unknown wrapResult:" + wrapResult);
                }
            } while (wrapMore);

            // The sendbuffer has received some data, so lets put it into reading mode
            // So it can be written to the socket
            sendBuffer.flip();
            long written = socketChannel.write(sendBuffer);
            metrics.incBytesWritten(written);

            boolean sendBufferDrained = !sendBuffer.hasRemaining();

            //System.out.println(TLSNioAsyncSocket.this + " bytes written:" + written);
            // Set the sendBuffer back to writing mode for more rounds of wrapping.
            compactOrClear(sendBuffer);

            if (ioVector.isEmpty() && sendBufferDrained) {
                // everything got written

                // clear the OP_WRITE flag if it was set
                int interestOps = key.interestOps();
                if ((interestOps & OP_WRITE) != 0) {
                    key.interestOps(interestOps & ~OP_WRITE);
                }

                resetFlushed();
            } else {
                // We need to register for the OP_WRITE because not everything got written
                key.interestOps(key.interestOps() | OP_WRITE);
            }
        }

        private boolean completeHandshake() throws IOException {
            while (true) {
                SSLEngineResult.HandshakeStatus handshakeStatus = sslEngine.getHandshakeStatus();
                //System.out.println(NioTlsAsyncSocket.this + " handshakeStatus " + handshakeStatus);

                switch (handshakeStatus) {
                    case NEED_TASK:
                        // todo: this is blocking, no good.
                        Runnable runnable = sslEngine.getDelegatedTask();
                        while (runnable != null) {
                            //System.out.println(TLSNioAsyncSocket.this + " handshakeStatus processing " + runnable);
                            runnable.run();
                            runnable = sslEngine.getDelegatedTask();
                        }
                        break;
                    case NEED_WRAP:
                        SSLEngineResult wrapResult = sslEngine.wrap(emptyBuffer, sendBuffer);
                        //System.out.println(TLSNioAsyncSocket.this + " handshake wrapResult " + wrapResult);
                        switch (wrapResult.getStatus()) {
                            case BUFFER_UNDERFLOW:
                                throw new RuntimeException("Buffer underflow");
                            case BUFFER_OVERFLOW:
                                throw new RuntimeException("Buffer overflow");
                            case CLOSED:
                                throw new EOFException("Remote socket closed!");
                            case OK:
                                sendBuffer.flip();
                                long written = socketChannel.write(sendBuffer);

                                metrics.incBytesWritten(written);
                                //System.out.println(TLSNioAsyncSocket.this + " handshakeStatus bytes written:" + written);
                                compactOrClear(sendBuffer);

                                // todo: we need to deal with situation that not everything got written

                                if (wrapResult.getHandshakeStatus() == FINISHED) {
                                    handshakeInProgress = false;
                                    sslSession = sslEngine.getSession();
                                    //System.out.println(TLSNioAsyncSocket.this + " handshake complete!!");
                                    resetFlushed();
                                    return true;
                                }

                                return false;
                            default:
                                throw new RuntimeException("Unknown wrapResult:" + wrapResult);
                        }
                    case NEED_UNWRAP:
                        int read = socketChannel.read(receiveBuffer);
                        //System.out.println(TLSNioAsyncSocket.this + " handshake bytes read: " + read);

                        if (read == -1) {
                            throw new EOFException("Remote socket closed!");
                        }

                        receiveBuffer.flip();
                        SSLEngineResult unwrapResult = sslEngine.unwrap(receiveBuffer, emptyBuffer);

                        compactOrClear(receiveBuffer);
                        //System.out.println(TLSNioAsyncSocket.this + " handshake unwrapResult " + unwrapResult);

                        switch (unwrapResult.getStatus()) {
                            case BUFFER_UNDERFLOW:
                                // not enough data is available to decode, so lets wait for more data.
                                return false;
                            case BUFFER_OVERFLOW:
                                throw new RuntimeException("Buffer overflow");
                            case CLOSED:
                                throw new RuntimeException("Closed");
                            case OK:
                                if (unwrapResult.getHandshakeStatus() == FINISHED) {
                                    //System.out.println(NioTlsAsyncSocket.this + "handshake complete!!");
                                    handshakeInProgress = false;
                                    sslSession = sslEngine.getSession();
                                    resetFlushed();
                                    return true;
                                }

                                break;
                            default:
                                throw new RuntimeException("Unknown unwrapResult:" + unwrapResult);
                        }
                        break;
                    case NOT_HANDSHAKING:
                        throw new IOException("Failed to complete the SSL/TLS handshake");
                    default:
                        // This also deals with FINISHED (which can only be returned on wrap/unwrap)
                        throw new IllegalStateException("Illegal handshakeStatus:" + handshakeStatus);
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
                logger.info("Connection established " + NioTlsAsyncSocket.this);
            }

            key.interestOps(key.interestOps() | OP_READ);
            connectFuture.complete(null);
            connectFuture = null;

            // we immediately need to schedule the handler so that the TLS handshake can start
            localTaskQueue.add(this);
        }
    }
}
