/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.tcp.nonblocking;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.operationexecutor.OperationHostileThread;
import com.hazelcast.util.counters.SwCounter;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.hazelcast.util.counters.SwCounter.newSwCounter;
import static java.lang.Math.max;
import static java.lang.System.currentTimeMillis;

public class NonBlockingIOThread extends Thread implements OperationHostileThread {

    // WARNING: This value has significant effect on idle CPU usage!
    private static final int SELECT_WAIT_TIME_MILLIS = 5000;
    private static final int SELECT_FAILURE_PAUSE_MILLIS = 1000;

    @Probe(name = "taskQueueSize")
    private final Queue<Runnable> taskQueue = new ConcurrentLinkedQueue<Runnable>();
    @Probe
    private final SwCounter eventCount = newSwCounter();

    private final ILogger logger;

    private final Selector selector;

    private final NonBlockingIOThreadOutOfMemoryHandler oomeHandler;

    private final boolean selectNow;

    private volatile long lastSelectTimeMs;

    public NonBlockingIOThread(ThreadGroup threadGroup,
                               String threadName,
                               ILogger logger,
                               NonBlockingIOThreadOutOfMemoryHandler oomeHandler) {
        this(threadGroup, threadName, logger, oomeHandler, false);
    }

    public NonBlockingIOThread(ThreadGroup threadGroup,
                               String threadName,
                               ILogger logger,
                               NonBlockingIOThreadOutOfMemoryHandler oomeHandler,
                               boolean selectNow) {
        super(threadGroup, threadName);
        this.logger = logger;
        this.selectNow = selectNow;
        this.oomeHandler = oomeHandler;
        try {
            selector = Selector.open();
        } catch (IOException e) {
            throw new HazelcastException("Failed to open a Selector", e);
        }
    }

    /**
     * Gets the Selector
     *
     * @return the Selector
     */
    public final Selector getSelector() {
        return selector;
    }

    /**
     * Returns the total number of selection-key events that have been processed by this thread.
     *
     * @return total number of selection-key events.
     */
    public long getEventCount() {
        return eventCount.get();
    }

    /**
     * A probe that measure how long this NonBlockingIOThread has not received any events.
     *
     * @return the idle time in ms.
     */
    @Probe
    private long idleTimeMs() {
        return max(currentTimeMillis() - lastSelectTimeMs, 0);
    }

    /**
     * Adds a task to this NonBlockingIOThread without notifying the thread.
     *
     * @param task the task to add
     * @throws NullPointerException if task is null
     */
    public final void addTask(Runnable task) {
        taskQueue.add(task);
    }

    /**
     * Adds a task to be executed by the NonBlockingIOThread and wakes up the selector so that it will
     * eventually pick up the task.
     *
     * @param task the task to add.
     * @throws NullPointerException if task is null
     */
    public final void addTaskAndWakeup(Runnable task) {
        taskQueue.add(task);
        if (!selectNow) {
            selector.wakeup();
        }
    }

    @Override
    public final void run() {
        // This outer loop is a bit complex but it takes care of a lot of stuff:
        // * it calls runSelectNowLoop or runSelectLoop based on selectNow enabled or not.
        // * handles backoff and retrying in case if io exception is thrown
        // * it takes care of other exception handling.
        //
        // The idea about this approach is that the runSelectNowLoop and runSelectLoop are as clean as possible and don't contain
        // any logic that isn't happening on the happy-path.
        try {
            for (; ; ) {
                try {
                    if (selectNow) {
                        runSelectNowLoop();
                    } else {
                        runSelectLoop();
                    }
                    // break the for loop; we are done
                    break;
                } catch (IOException nonFatalException) {
                    // an IOException happened, we are going to do some waiting and retry.
                    // If we don't wait, it can be that a subsequent call will run into an IOException immediately.
                    // This can lead to a very hot loop and we don't want that. The same approach is used in Netty.

                    logger.warning(getName() + " " + nonFatalException.toString(), nonFatalException);

                    try {
                        Thread.sleep(SELECT_FAILURE_PAUSE_MILLIS);
                    } catch (InterruptedException i) {
                        interrupt();
                    }
                }
            }
        } catch (OutOfMemoryError e) {
            oomeHandler.handle(e);
        } catch (Throwable e) {
            logger.warning("Unhandled exception in " + getName(), e);
        } finally {
            closeSelector();
        }

        logger.finest(getName() + " finished");
    }

    private void runSelectLoop() throws IOException {
        while (!isInterrupted()) {
            processTaskQueue();

            int selectedKeys = selector.select(SELECT_WAIT_TIME_MILLIS);
            if (selectedKeys > 0) {
                lastSelectTimeMs = currentTimeMillis();
                handleSelectionKeys();
            }
        }
    }

    private void runSelectNowLoop() throws IOException {
        while (!isInterrupted()) {
            processTaskQueue();

            int selectedKeys = selector.selectNow();
            if (selectedKeys > 0) {
                lastSelectTimeMs = currentTimeMillis();
                handleSelectionKeys();
            }
        }
    }

    private void processTaskQueue() {
        while (!isInterrupted()) {
            Runnable task = taskQueue.poll();
            if (task == null) {
                return;
            }
            executeTask(task);
        }
    }

    private void executeTask(Runnable task) {
        NonBlockingIOThread target = getTargetIoThread(task);
        if (target == this) {
            task.run();
        } else {
            target.addTask(task);
        }
    }

    private NonBlockingIOThread getTargetIoThread(Runnable task) {
        if (task instanceof MigratableHandler) {
            return ((MigratableHandler) task).getOwner();
        } else {
            return this;
        }
    }

    private void handleSelectionKeys() {
        Iterator<SelectionKey> it = selector.selectedKeys().iterator();
        while (it.hasNext()) {
            SelectionKey sk = it.next();
            it.remove();

            handleSelectionKey(sk);
        }
    }

    protected void handleSelectionKey(SelectionKey sk) {
        if (!sk.isValid()) {
            return;
        }

        // we don't need to check for sk.isReadable/sk.isWritable since the handler has only registered for events it can handle.

        eventCount.inc();
        SelectionHandler handler = (SelectionHandler) sk.attachment();
        try {
            handler.handle();
        } catch (Throwable t) {
            handler.onFailure(t);
        }
    }

    public void handleSelectionKeyFailure(Throwable e) {
        logger.warning("Selector exception at  " + getName() + ", cause= " + e.toString(), e);
        if (e instanceof OutOfMemoryError) {
            oomeHandler.handle((OutOfMemoryError) e);
        }
    }

    private void closeSelector() {
        if (logger.isFinestEnabled()) {
            logger.finest("Closing selector for:" + getName());
        }

        try {
            selector.close();
        } catch (Exception e) {
            logger.finest("Failed to close selector", e);
        }
    }

    public final void shutdown() {
        taskQueue.clear();
        interrupt();
    }

    @Override
    public String toString() {
        return getName();
    }
}
