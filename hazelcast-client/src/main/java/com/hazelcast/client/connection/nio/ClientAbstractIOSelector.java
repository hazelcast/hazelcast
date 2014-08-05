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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.tcp.IOSelector;
import com.hazelcast.util.EmptyStatement;

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

    private static final int SHUTDOWN_TIMEOUT_SECONDS = 3;
    private static final int SELECT_WAIT_TIME_MILLIS = 5000;
    private static final int SELECT_FAILURE_PAUSE_MILLIS = 1000;

    protected final ILogger logger;

    protected final Queue<Runnable> selectorQueue = new ConcurrentLinkedQueue<Runnable>();

    protected final int waitTime;

    protected final Selector selector;

    protected boolean live = true;

    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    /**
     * Creates a ClientAbstractIOSelector
     *
     * @param threadGroup
     * @param threadName
     * @throws com.hazelcast.core.HazelcastException if no Selector could be opened.
     */
    protected ClientAbstractIOSelector(ThreadGroup threadGroup, String threadName) {
        super(threadGroup, threadName);
        this.logger = Logger.getLogger(getClass().getName());
        this.waitTime = SELECT_WAIT_TIME_MILLIS;
        try {
            selector = Selector.open();
        } catch (final IOException e) {
            throw new HazelcastException("Failed to open a Selector", e);
        }
    }

    @Override
    public Selector getSelector() {
        return selector;
    }

    @Override
    public void addTask(Runnable runnable) {
        selectorQueue.add(runnable);
    }

    @Override
    public void wakeup() {
        selector.wakeup();
    }

    @Override
    public void shutdown() {
        selectorQueue.clear();
        try {
            addTask(new Runnable() {
                @Override
                public void run() {
                    live = false;
                    shutdownLatch.countDown();
                }
            });
            interrupt();
        } catch (Throwable ignored) {
            EmptyStatement.ignore(ignored);
        }
    }

    @Override
    public void awaitShutdown() {
        try {
            shutdownLatch.await(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
            EmptyStatement.ignore(ignored);
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

    @Override
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
                    handleSelectFailure(e);
                    continue;
                }

                if (selectedKeyCount == 0) {
                    continue;
                }
                handleSelectionKeys();
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
                EmptyStatement.ignore(ignored);
            }
        }
    }

    private void handleSelectFailure(Throwable e) {
        logger.warning(e.toString(), e);

        // If we don't wait, it can be that a subsequent call will run into an IOException immediately. This can lead to a very
        // hot loop and we don't want that. The same approach is used in Netty.
        try {
            Thread.sleep(SELECT_FAILURE_PAUSE_MILLIS);
        } catch (InterruptedException i) {
            Thread.currentThread().interrupt();
        }
    }

    private void handleSelectionKeys() {
        final Set<SelectionKey> setSelectedKeys = selector.selectedKeys();
        final Iterator<SelectionKey> it = setSelectedKeys.iterator();
        while (it.hasNext()) {
            final SelectionKey sk = it.next();
            it.remove();

            try {
                handleSelectionKey(sk);
            } catch (Throwable e) {
                handleSelectionKeyFailure(e);
            }
        }
    }

    protected abstract void handleSelectionKey(SelectionKey sk);

    private void handleSelectionKeyFailure(final Throwable e) {
        String msg = "SelectionKey exception at  " + getName() + ", cause= " + e.toString();
        logger.warning(msg, e);
    }
}
