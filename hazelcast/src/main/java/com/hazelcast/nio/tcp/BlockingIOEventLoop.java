package com.hazelcast.nio.tcp;

import java.nio.channels.SelectionKey;
import java.util.Iterator;
import java.util.Set;

/**
 * Default {@link com.hazelcast.nio.tcp.IOEventLoop} implementation.
 * <p/>
 * This loop makes use of the {@link java.nio.channels.Selector#select()} method and won't do any spinning.
 */
public final class BlockingIOEventLoop extends AbstractIOEventLoop {

    // WARNING: This value has significant effect on idle CPU usage!
    private static final int SELECT_WAIT_TIME_MILLIS = 5000;

    public BlockingIOEventLoop(IOReactor reactor) {
        super(reactor);
    }

    @Override
    public void dumpPerformanceMetrics(StringBuffer sb) {
        sb.append(reactor.getName())
                .append(".writeEvents=").append(writeEvents).append(' ')
                .append(".readEvents=").append(readEvents)
                .append("\n");
    }

    @Override
    public void run() {
        Thread currentThread = Thread.currentThread();

        //noinspection WhileLoopSpinsOnField
        while (reactor.isAlive()) {
            reactor.processTasks();

             if (!reactor.isAlive() || currentThread.isInterrupted()) {
                if (logger.isFinestEnabled()) {
                    logger.finest(reactor.getName() + " is interrupted!");
                }
                return;
            }

            int selectedKeyCount;
            try {
                selectedKeyCount = selector.select(SELECT_WAIT_TIME_MILLIS);
            } catch (Throwable e) {
                handleSelectFailure(e);
                continue;
            }

            if (selectedKeyCount == 0) {
                continue;
            }

            handleSelectionKeys();
        }
    }

     private void handleSelectionKeys() {
        Set<SelectionKey> setSelectedKeys = selector.selectedKeys();
        Iterator<SelectionKey> it = setSelectedKeys.iterator();
        while (it.hasNext()) {
            SelectionKey sk = it.next();
            it.remove();
            try {
                handleSelectionKey(sk);
            } catch (Throwable e) {
                handleSelectionKeyFailure(e);
            }
        }
    }
}
