package com.hazelcast.nio.tcp;

import com.hazelcast.logging.ILogger;

import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;

/**
 * Abstract {@link com.hazelcast.nio.tcp.IOEventLoop} implementation that provides lot of the basic pluming logic.
 */
public abstract class AbstractIOEventLoop implements IOEventLoop {

    private static final int SELECT_FAILURE_PAUSE_MILLIS = 1000;

    protected final IOReactor reactor;
    protected final ILogger logger;
    protected final Selector selector;
    protected final IOReactorOutOfMemoryHandler oomeHandler;

    // These 2 field will be incremented only the the reactor-thread, but can read by multiple threads.
    protected volatile long readEvents;
    protected volatile long writeEvents;

    public AbstractIOEventLoop(IOReactor reactor) {
        this.reactor = reactor;
        this.logger = reactor.getLogger();
        this.selector = reactor.getSelector();
        this.oomeHandler = reactor.getOutOfMemoryHandler();
    }

    @Override
    public void dumpPerformanceMetrics(StringBuffer sb) {
        sb.append(reactor.getName())
                .append(".writeEvents=").append(writeEvents).append(' ')
                .append(".readEvents=").append(readEvents)
                .append("\n");
    }


    @edu.umd.cs.findbugs.annotations.SuppressWarnings({"VO_VOLATILE_INCREMENT" })
    protected final void handleSelectionKey(SelectionKey sk) {
        if (sk.isValid() && sk.isWritable()) {
            writeEvents++;
            sk.interestOps(sk.interestOps() & ~SelectionKey.OP_WRITE);
            IOEventHandler handler = (IOEventHandler) sk.attachment();
            handler.handle();
        }

        if (sk.isValid() && sk.isReadable()) {
            readEvents++;
            IOEventHandler handler = (IOEventHandler) sk.attachment();
            handler.handle();
        }
    }

    protected final void handleSelectionKeyFailure(Throwable e) {
        String msg = "Reactor exception from  " + reactor.getName() + ", cause= " + e.toString();
        logger.warning(msg, e);
        if (e instanceof OutOfMemoryError) {
            oomeHandler.handle((OutOfMemoryError) e);
        }
    }

    protected final void handleSelectFailure(Throwable e) {
        logger.warning(e.toString(), e);

        // If we don't wait, it can be that a subsequent call will run into an IOException immediately. This can lead to a very
        // hot loop and we don't want that. The same approach is used in Netty.
        try {
            Thread.sleep(SELECT_FAILURE_PAUSE_MILLIS);
        } catch (InterruptedException i) {
            Thread.currentThread().interrupt();
        }
    }
}

