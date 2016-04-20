/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.operationexecutor.OperationHostileThread;
import com.hazelcast.util.EmptyStatement;

import java.io.IOException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static com.hazelcast.nio.tcp.nonblocking.SelectorOptimizer.optimize;
import static java.lang.Math.max;
import static java.lang.System.currentTimeMillis;

public class NonBlockingIOThread extends Thread implements OperationHostileThread {

    // WARNING: This value has significant effect on idle CPU usage!
    private static final int SELECT_WAIT_TIME_MILLIS = 5000;
    private static final int SELECT_FAILURE_PAUSE_MILLIS = 1000;
    // When we detect Selector.select returning prematurely
    // for more than SELECT_IDLE_COUNT_THRESHOLD then we rebuild the selector
    private static final int SELECT_IDLE_COUNT_THRESHOLD = 10;
    // for tests only
    private static final Random RANDOM = new Random();
    // when testing, we simulate the selector bug randomly with one out of TEST_SELECTOR_BUG_PROBABILITY
    private static final int TEST_SELECTOR_BUG_PROBABILITY = Integer.parseInt(
            System.getProperty("hazelcast.io.selector.bug.probability", "16"));

    @SuppressWarnings("checkstyle:visibilitymodifier")
    // this field is set during construction and is meant for the probes so that the read/write handler can
    // indicate which thread they are currently bound to.
    @Probe(name = "ioThreadId", level = ProbeLevel.INFO)
    public int id;

    @Probe(name = "taskQueueSize")
    private final Queue<Runnable> taskQueue = new ConcurrentLinkedQueue<Runnable>();
    @Probe
    private final SwCounter eventCount = newSwCounter();
    @Probe
    private final SwCounter selectorIOExceptionCount = newSwCounter();
    @Probe
    private final SwCounter completedTaskCount = newSwCounter();
    // count number of times the selector was rebuilt (if selectWorkaround is enabled)
    @Probe
    private final SwCounter selectorRebuildCount = newSwCounter();

    private final ILogger logger;

    private Selector selector;

    private final NonBlockingIOThreadOutOfMemoryHandler oomeHandler;

    private final SelectorMode selectMode;

    // last time select unblocked with some keys selected
    private volatile long lastSelectTimeMs;
    // set to true while testing
    private boolean selectorWorkaroundTest;

    public NonBlockingIOThread(ThreadGroup threadGroup,
                               String threadName,
                               ILogger logger,
                               NonBlockingIOThreadOutOfMemoryHandler oomeHandler) {
        this(threadGroup, threadName, logger, oomeHandler, SelectorMode.SELECT);
    }

    public NonBlockingIOThread(ThreadGroup threadGroup,
                               String threadName,
                               ILogger logger,
                               NonBlockingIOThreadOutOfMemoryHandler oomeHandler,
                               SelectorMode selectMode) {
        this(threadGroup, threadName, logger, oomeHandler, selectMode, newSelector(logger));
    }

    public NonBlockingIOThread(ThreadGroup threadGroup,
                               String threadName,
                               ILogger logger,
                               NonBlockingIOThreadOutOfMemoryHandler oomeHandler,
                               SelectorMode selectMode,
                               Selector selector) {
        super(threadGroup, threadName);
        this.logger = logger;
        this.selectMode = selectMode;
        this.oomeHandler = oomeHandler;
        this.selector = selector;
        this.selectorWorkaroundTest = false;
    }

    private static Selector newSelector(ILogger logger) {
        try {
            Selector selector = Selector.open();
            if (Boolean.getBoolean("tcp.optimizedselector")) {
                optimize(selector, logger);
            }
            return selector;
        } catch (final IOException e) {
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
    public void addTaskAndWakeup(Runnable task) {
        taskQueue.add(task);
        if (selectMode != SelectorMode.SELECT_NOW) {
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
                    switch (selectMode) {
                        case SELECT_WITH_FIX:
                            selectLoopWithFix();
                            break;
                        case SELECT_NOW:
                            selectNowLoop();
                            break;
                        case SELECT:
                            selectLoop();
                            break;
                        default:
                            throw new IllegalArgumentException("Selector.select mode not set, use -Dhazelcast.io.selectorMode="
                                    + "{select|selectnow|selectwithfix} to explicitly specify select mode or leave empty for "
                                    + "default select mode.");
                    }
                    // break the for loop; we are done
                    break;
                } catch (IOException nonFatalException) {
                    selectorIOExceptionCount.inc();
                    logger.warning(getName() + " " + nonFatalException.toString(), nonFatalException);
                    coolDown();
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

    /**
     * When an IOException happened, the loop is going to be retried but we need to wait a bit
     * before retrying. If we don't wait, it can be that a subsequent call will run into an IOException
     * immediately. This can lead to a very hot loop and we don't want that. A similar approach is used
     * in Netty
     */
    private void coolDown() {
        try {
            Thread.sleep(SELECT_FAILURE_PAUSE_MILLIS);
        } catch (InterruptedException i) {
            // if the thread is interrupted, we just restore the interrupt flag and let one of the loops deal with it
            interrupt();
        }
    }

    private void selectLoop() throws IOException {
        while (!isInterrupted()) {
            processTaskQueue();

            int selectedKeys = selector.select(SELECT_WAIT_TIME_MILLIS);
            if (selectedKeys > 0) {
                handleSelectionKeys();
            }
        }
    }

    private void selectLoopWithFix() throws IOException {
        int idleCount = 0;
        while (!isInterrupted()) {
            processTaskQueue();

            long before = currentTimeMillis();
            int selectedKeys = selector.select(SELECT_WAIT_TIME_MILLIS);
            if (selectedKeys > 0) {
                idleCount = 0;
                handleSelectionKeys();
            } else if (!taskQueue.isEmpty()) {
                idleCount = 0;
            } else {
                // no keys were selected, not interrupted by wakeup therefore we hit an issue with JDK/network stack
                long selectTimeTaken = currentTimeMillis() - before;
                idleCount = selectTimeTaken < SELECT_WAIT_TIME_MILLIS ? idleCount + 1 : 0;

                if (selectorBugDetected(idleCount)) {
                    rebuildSelector();
                    idleCount = 0;
                }
            }
        }
    }

    private boolean selectorBugDetected(int idleCount) {
        return idleCount > SELECT_IDLE_COUNT_THRESHOLD
                || (selectorWorkaroundTest && RANDOM.nextInt(TEST_SELECTOR_BUG_PROBABILITY) == 1);
    }

    private void selectNowLoop() throws IOException {
        while (!isInterrupted()) {
            processTaskQueue();

            int selectedKeys = selector.selectNow();
            if (selectedKeys > 0) {
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
        completedTaskCount.inc();

        NonBlockingIOThread target = getTargetIOThread(task);
        if (target == this) {
            task.run();
        } else {
            target.addTaskAndWakeup(task);
        }
    }

    private NonBlockingIOThread getTargetIOThread(Runnable task) {
        if (task instanceof MigratableHandler) {
            return ((MigratableHandler) task).getOwner();
        } else {
            return this;
        }
    }

    private void handleSelectionKeys() {
        lastSelectTimeMs = currentTimeMillis();
        Iterator<SelectionKey> it = selector.selectedKeys().iterator();
        while (it.hasNext()) {
            SelectionKey sk = it.next();
            it.remove();

            handleSelectionKey(sk);
        }
    }

    protected void handleSelectionKey(SelectionKey sk) {
        SelectionHandler handler = (SelectionHandler) sk.attachment();
        try {
            if (!sk.isValid()) {
                // if the selectionKey isn't valid, we throw this exception to feedback the situation into the handler.onFailure
                throw new CancelledKeyException();
            }

            // we don't need to check for sk.isReadable/sk.isWritable since the handler has only registered
            // for events it can handle.
            eventCount.inc();
            handler.handle();
        } catch (Throwable t) {
            handler.onFailure(t);
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

    // this method is always invoked in this thread
    // after we have blocked for selector.select in #runSelectLoopWithSelectorFix
    private void rebuildSelector() {
        selectorRebuildCount.inc();
        Selector newSelector = newSelector(logger);
        Selector oldSelector = this.selector;

        // reset each handler's selectionKey, cancel the old keys
        for (SelectionKey key : oldSelector.keys()) {
            AbstractHandler handler = (AbstractHandler) key.attachment();
            SelectableChannel channel = key.channel();
            try {
                int ops = key.interestOps();
                SelectionKey newSelectionKey = channel.register(newSelector, ops, handler);
                handler.setSelectionKey(newSelectionKey);
            } catch (ClosedChannelException e) {
                logger.info("Channel was closed while trying to register with new selector.");
            } catch (CancelledKeyException e) {
                // a CancelledKeyException may be thrown in key.interestOps
                // in this case, since the key is already cancelled, just do nothing
                EmptyStatement.ignore(e);
            }
            key.cancel();
        }

        // close the old selector and substitute with new one
        closeSelector();
        this.selector = newSelector;
        logger.warning("Recreated Selector because of possible java/network stack bug.");
    }

    @Override
    public String toString() {
        return getName();
    }

    void setSelectorWorkaroundTest(boolean selectorWorkaroundTest) {
        this.selectorWorkaroundTest = selectorWorkaroundTest;
    }
}
