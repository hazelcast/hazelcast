/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.RuntimeInterruptedException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public final class InOutSelector extends Thread implements Runnable {

    final static long TEN_SECOND_MILLIS = TimeUnit.SECONDS.toMillis(10);

    private final ILogger logger;

    private final Queue<Runnable> selectorQueue = new ConcurrentLinkedQueue<Runnable>();

    private final ConnectionManager connectionManager;

    private final int waitTime;

    private boolean live = true;

    private long lastPublish = 0;

    final Selector selector;

    public InOutSelector(ConnectionManager connectionManager, int id) {
        super(connectionManager.ioService.getThreadGroup(), connectionManager.ioService.getThreadPrefix() + id);
        this.connectionManager = connectionManager;
        this.logger = connectionManager.ioService.getLogger(this.getClass().getName());
        this.waitTime = 5000;  // TODO: This value has significant effect on idle CPU usage!
        Selector selectorTemp = null;
        try {
            selectorTemp = Selector.open();
        } catch (final IOException e) {
            handleSelectorException(e);
        }
        this.selector = selectorTemp;
        live = true;
    }

    public void shutdown() {
        selectorQueue.clear();
        try {
            final CountDownLatch l = new CountDownLatch(1);
            addTask(new Runnable() {
                public void run() {
                    live = false;
                    threadLocalShutdown();
                    l.countDown();
                }
            });
            interrupt();
            l.await(3, TimeUnit.SECONDS);
        } catch (Throwable ignored) {
        }
    }

    protected void threadLocalShutdown() {
    }

    public void addTask(final Runnable runnable) {
        selectorQueue.offer(runnable);
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

    public void publishUtilization() {
    }

    public final void run() {
        try {
            connectionManager.ioService.onIOThreadStart();
            //noinspection WhileLoopSpinsOnField
            while (live) {
                long currentMillis = Clock.currentTimeMillis();
                if ((currentMillis - lastPublish) > TEN_SECOND_MILLIS) {
                    publishUtilization();
                    lastPublish = currentMillis;
                }
                processSelectionQueue();
                if (!live) return;
                if (isInterrupted()) {
                    connectionManager.ioService.handleInterruptedException(this, new RuntimeInterruptedException());
                    live = false;
                    return;
                }
                int selectedKeyCount;
                try {
                    selectedKeyCount = selector.select(waitTime);
                } catch (Throwable exp) {
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
                        if (sk.isValid() && sk.isReadable()) {
                            Connection connection = (Connection) sk.attachment();
                            connection.getReadHandler().handle();
                        }
                        if (sk.isValid() && sk.isWritable()) {
                            sk.interestOps(sk.interestOps() & ~SelectionKey.OP_WRITE);
                            Connection connection = (Connection) sk.attachment();
                            connection.getWriteHandler().handle();
                        }
                    } catch (Throwable e) {
                        handleSelectorException(e);
                    }
                }
            }
        } catch (OutOfMemoryError e) {
            connectionManager.ioService.onOutOfMemory(e);
        } catch (Throwable e) {
            logger.log(Level.WARNING, "unhandled exception in " + Thread.currentThread().getName(), e);
        } finally {
            try {
                logger.log(Level.FINEST, "closing selector " + Thread.currentThread().getName());
                selector.close();
            } catch (final Exception ignored) {
            }
        }
    }

    protected void handleSelectorException(final Throwable e) {
        String msg = "Selector exception at  " + Thread.currentThread().getName() + ", cause= " + e.toString();
        logger.log(Level.WARNING, msg, e);
        if (e instanceof OutOfMemoryError) {
            connectionManager.ioService.onOutOfMemory((OutOfMemoryError) e);
        }
    }
}
