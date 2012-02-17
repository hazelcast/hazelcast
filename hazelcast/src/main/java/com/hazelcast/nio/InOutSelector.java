/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

import com.hazelcast.logging.ILogger;
import com.hazelcast.util.ThreadWatcher;

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

public final class InOutSelector implements Runnable {

    protected final ILogger logger;

    protected final Selector selector;

    protected final Queue<Runnable> selectorQueue = new ConcurrentLinkedQueue<Runnable>();

    protected final ConnectionManager connectionManager;

    private final int waitTime;

    protected boolean live = true;

    protected final ThreadWatcher threadWatcher = new ThreadWatcher();

    final static long TEN_SECOND_MILLIS = TimeUnit.SECONDS.toMillis(10);

    private long lastPublish = 0;

    public InOutSelector(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
        logger = connectionManager.ioService.getLogger(this.getClass().getName());
        this.waitTime = 1;
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
            l.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
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
                threadWatcher.incrementRunCount();
                long currentMillis = System.currentTimeMillis();
                if ((currentMillis - lastPublish) > TEN_SECOND_MILLIS) {
                    publishUtilization();
                    lastPublish = currentMillis;
                }
                processSelectionQueue();
                if (!live) return;
                int selectedKeyCount;
                try {
                    long startWait = System.nanoTime();
                    selectedKeyCount = selector.select(waitTime);
                    long now = System.nanoTime();
                    threadWatcher.addWait((now - startWait), now);
                    if (Thread.interrupted()) {
                        connectionManager.ioService.handleInterruptedException(Thread.currentThread(), new RuntimeException());
                        return;
                    }
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
//                        if (sk.isValid() && sk.isConnectable()) {
//                            sk.interestOps(sk.interestOps() & ~SelectionKey.OP_CONNECT);
//                            SelectionHandler selectionHandler = (SelectionHandler) sk.attachment();
//                            selectionHandler.handle();
//                        }
//                        if (sk.isValid() && sk.isAcceptable()) {
//                            SelectionHandler selectionHandler = (SelectionHandler) sk.attachment();
//                            selectionHandler.handle();
//                        }
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
    }
}
