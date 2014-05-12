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

package com.hazelcast.client.connection.nio;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.IOSelector;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public abstract class ClientAbstractIOSelector extends Thread implements IOSelector {

    private static final int TIMEOUT = 3;

    protected final ILogger logger;

    protected final Queue<Runnable> selectorQueue = new ConcurrentLinkedQueue<Runnable>();

    protected final int waitTime;

    protected final Selector selector;

    protected boolean live = true;

    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    protected ClientAbstractIOSelector(ThreadGroup threadGroup, String threadName) {
        super(threadGroup, threadName);
        this.logger = Logger.getLogger(getClass().getName());
        this.waitTime = 5000;
        Selector selectorTemp = null;
        try {
            selectorTemp = Selector.open();
        } catch (final IOException e) {
            handleSelectorException(e);
        }
        this.selector = selectorTemp;
    }

    public Selector getSelector() {
        return selector;
    }

    public void addTask(Runnable runnable) {
        selectorQueue.add(runnable);
    }

    public void wakeup() {
        selector.wakeup();
    }

    public void shutdown() {
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

    public void awaitShutdown() {
        try {
            shutdownLatch.await(TIMEOUT, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
        }
    }

    private void processSelectionQueue() {
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
                        logger.finest(getName() + " is interrupted!");
                    }
                    live = false;
                    return;
                }
                int selectedKeyCount;
                try {
                    selectedKeyCount = selector.select(waitTime);
                } catch (Throwable e) {
                    logger.warning(e.toString());
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
                        handleSelectorException(e);
                    }
                }
            }
        } catch (Throwable e) {
            logger.warning("Unhandled exception in " + getName(), e);
        } finally {
            try {
                if (logger.isFinestEnabled()) {
                    logger.finest("Closing selector " + getName());
                }
                selector.close();
            } catch (final Exception ignored) {
            }
        }
    }

    protected abstract void handleSelectionKey(SelectionKey sk);

    private void handleSelectorException(final Throwable e) {
        String msg = "Selector exception at  " + getName() + ", cause= " + e.toString();
        logger.warning(msg, e);
    }
}
