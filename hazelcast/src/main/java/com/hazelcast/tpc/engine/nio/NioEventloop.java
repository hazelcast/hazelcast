package com.hazelcast.tpc.engine.nio;

import com.hazelcast.internal.networking.nio.SelectorOptimizer;
import com.hazelcast.tpc.engine.Eventloop;
import com.hazelcast.tpc.engine.Util;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.tpc.engine.EventloopState.RUNNING;
import static com.hazelcast.tpc.engine.Util.epochNanos;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public final class NioEventloop extends Eventloop {
    final Selector selector;

    public NioEventloop() {
        this.selector = SelectorOptimizer.newSelector(logger);
    }

    @Override
    public void wakeup() {
        if (spin || Thread.currentThread() == this) {
            return;
        }

        if (wakeupNeeded.get() && wakeupNeeded.compareAndSet(true, false)) {
            selector.wakeup();
        }
    }

    @Override
    protected void eventLoop() throws Exception {
        while (state == RUNNING) {
            runConcurrentTasks();

            boolean moreWork = scheduler.tick();

            runLocalTasks();

            int keyCount;
            if (spin || moreWork) {
                keyCount = selector.selectNow();
            } else {
                wakeupNeeded.set(true);
                if (concurrentRunQueue.isEmpty()) {
                    if (earliestDeadlineEpochNanos == -1) {
                        keyCount = selector.select();
                    } else {
                        long timeoutNanos = earliestDeadlineEpochNanos - epochNanos();
                        if (timeoutNanos < 0) {
                            timeoutNanos = 0;
                        }

                        keyCount = selector.select(NANOSECONDS.toMillis(timeoutNanos));
                    }
                } else {
                    keyCount = selector.selectNow();
                }
                wakeupNeeded.set(false);
            }

            if (keyCount > 0) {
                Iterator<SelectionKey> it = selector.selectedKeys().iterator();
                while (it.hasNext()) {
                    SelectionKey key = it.next();
                    it.remove();

                    NioSelectedKeyListener listener = (NioSelectedKeyListener) key.attachment();
                    try {
                        listener.handle(key);
                    } catch (IOException e) {
                        listener.handleException(e);
                    }
                }
            }
        }
    }
}
