/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.nio;

import com.hazelcast.impl.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.util.ThreadWatcher;

import java.io.IOException;
import java.net.Socket;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public abstract class SelectorBase implements Runnable {

    protected final ILogger logger;

    protected final Selector selector;

    protected final Queue<Runnable> selectorQueue = new ConcurrentLinkedQueue<Runnable>();

    protected final Node node;

    private final int waitTime;

    protected boolean live = true;

    protected final ThreadWatcher threadWatcher = new ThreadWatcher();

    public SelectorBase(Node node, int waitTime) {
        this.node = node;
        logger = node.getLogger(this.getClass().getName());
        this.waitTime = waitTime;
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
        if (selectorQueue != null) {
            selectorQueue.clear();
        }
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

    public void processSelectionQueue() {
        while (live) {
            final Runnable runnable = selectorQueue.poll();
            if (runnable == null) {
                return;
            }
            runnable.run();
        }
    }

    public abstract void publishUtilization();

    public final void run() {
        try {
            while (live) {
                if (threadWatcher.incrementRunCount() % 10000 == 0) {
                    publishUtilization();
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
                        node.handleInterruptedException(Thread.currentThread(), new RuntimeException());
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
                    it.remove();
                    try {
                        sk.interestOps(sk.interestOps() & ~sk.readyOps());
                        SelectionHandler selectionHandler = (SelectionHandler) sk.attachment();
                        selectionHandler.handle();
                    } catch (CancelledKeyException e) {
                        // nothing do
                    } catch (Throwable e) {
                        handleSelectorException(e);
                        //break;
                    }
                }
            }
        } catch (Throwable e) {
            logger.log(Level.WARNING, "unhandled exception in " + Thread.currentThread().getName(), e);
        } finally {
            try {
                logger.log(Level.FINE, "closing selector " + Thread.currentThread().getName());
                selector.close();
            } catch (final Exception ignored) {
            }
        }
    }

    protected void handleSelectorException(final Throwable e) {
        String msg = "Selector exception at  " + Thread.currentThread().getName() + ", cause= " + e.toString();
        logger.log(Level.WARNING, msg, e);
    }

    protected void initSocket(Socket socket) throws Exception {
        socket.setKeepAlive(true);
        //socket.setTcpNoDelay(true);
        socket.setSoLinger(true, 1);
        socket.setReceiveBufferSize(node.connectionManager.SOCKET_RECEIVE_BUFFER_SIZE);
        socket.setSendBufferSize(node.connectionManager.SOCKET_SEND_BUFFER_SIZE);
    }

    protected Connection createConnection(final SocketChannel socketChannel, final boolean acceptor)
            throws Exception {
        return node.connectionManager.createConnection(socketChannel, acceptor);
    }
}
