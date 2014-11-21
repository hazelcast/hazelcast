package com.hazelcast.nio.tcp;

import java.nio.channels.SelectionKey;
import java.util.Iterator;
import java.util.Set;

/**
 * A {link AbstractIOEventLoop} that spins on the Selector. So it doesn't call {@link java.nio.channels.Selector#select()};
 * but calls the {@link java.nio.channels.Selector#selectNow()} method.
 *
 * This functionality is currently experimental and disabled by default. Using spinning reduces latency at the expense of
 * increased CPU overhead. And this could slow down the application (since more cycles are wasted).
 */
public final class SpinningIOEventLoop extends AbstractIOEventLoop {

    public SpinningIOEventLoop(IOReactor reactor) {
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
                selectedKeyCount = selector.selectNow();
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
