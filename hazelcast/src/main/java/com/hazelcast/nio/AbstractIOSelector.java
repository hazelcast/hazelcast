/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.nio;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

abstract class AbstractIOSelector extends Thread implements IOSelector {

    protected final ILogger logger;

    protected final Queue<Runnable> selectorQueue = new ConcurrentLinkedQueue<Runnable>();

    protected final IOService ioService;

    protected final int waitTime;

    protected final Selector selector;

    protected boolean live = true;

    protected AbstractIOSelector(IOService ioService, String tname) {
        super(ioService.getThreadGroup(), tname);
        this.ioService = ioService;
        this.logger = ioService.getLogger(getClass().getName());
        this.waitTime = 5000;  // WARNING: This value has significant effect on idle CPU usage!
        try {
            selector = Selector.open();
        } catch (final IOException e) {
            throw new HazelcastException("Failed to open a Selector", e);
        }
    }

    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    public final void shutdown() {
        selectorQueue.clear();
        try {
            addTask(new Runnable() {
                public void run() {
                    live = false;
                    shutdownLatch.countDown();
                }
            });
            interrupt();
        } catch (Throwable ignored) {
        }
    }

    public final void awaitShutdown() {
        try {
            shutdownLatch.await(3, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
        }
    }

    public final void addTask(Runnable runnable) {
        selectorQueue.add(runnable);
    }

    private void processSelectionQueue() {
        //noinspection WhileLoopSpinsOnField
        while (live) {
            final Runnable runnable = selectorQueue.poll();
            if (runnable == null) {
                return;
            }
            runnable.run();
        }
    }

    public final void run() {
        try {
            //noinspection WhileLoopSpinsOnField
            while (live) {
                processSelectionQueue();
                if (!live || isInterrupted()) {
                    if (logger.isFinestEnabled()) {
                        logger.finest( getName() + " is interrupted!");
                    }
                    live = false;
                    return;
                }
                int selectedKeyCount;
                try {
                    selectedKeyCount = selector.select(waitTime);
                } catch (Throwable e) {
                    handleSelectFailure(e);
                    continue;
                }
                if (selectedKeyCount == 0) {
                    continue;
                }
                final Set<SelectionKey> setSelectedKeys = selector.selectedKeys();
                final Iterator<SelectionKey> it = setSelectedKeys.iterator();
                while (it.hasNext()) {
                    final SelectionKey sk = it.next();
                    try {
                        it.remove();
                        handleSelectionKey(sk);
                    } catch (Throwable e) {
                        logException(e);
                    }
                }
            }
        } catch (OutOfMemoryError e) {
            ioService.onOutOfMemory(e);
        } catch (Throwable e) {
            logger.warning("Unhandled exception in " + getName(), e);
        } finally {
            try {
                if (logger.isFinestEnabled()) {
                    logger.finest( "Closing selector " + getName());
                }
                selector.close();
            } catch (final Exception ignored) {
            }
        }
    }

    protected abstract void handleSelectionKey(SelectionKey sk);

    private void logException(final Throwable e) {
        String msg = "Selector exception at  " + getName() + ", cause= " + e.toString();
        logger.warning(msg, e);
        if (e instanceof OutOfMemoryError) {
            ioService.onOutOfMemory((OutOfMemoryError) e);
        }
    }

    public final Selector getSelector() {
        return selector;
    }

    public final void wakeup() {
        selector.wakeup();
    }

    private void handleSelectFailure(Throwable e) {
        logger.warning(e.toString(), e);

        // If we don't wait, it can be that a subsequent call will run into an IOException immediately. This can lead to a very
        // hot loop and we don't want that. The same approach is used in Netty.
        try {
            Thread.sleep(1000);
        } catch (InterruptedException i) {
            Thread.currentThread().interrupt();
        }
    }
}
