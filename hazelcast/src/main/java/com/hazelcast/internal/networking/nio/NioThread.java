/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.networking.nio;

import com.hazelcast.internal.metrics.ExcludedMetricTargets;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.networking.ChannelErrorHandler;
import com.hazelcast.internal.util.concurrent.IdleStrategy;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.internal.util.executor.HazelcastManagedThread;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.operationexecutor.OperationHostileThread;

import java.io.IOException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.NETWORKING_METRIC_NIO_THREAD_BYTES_TRANSCEIVED;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.NETWORKING_METRIC_NIO_THREAD_COMPLETED_TASK_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.NETWORKING_METRIC_NIO_THREAD_EVENT_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.NETWORKING_METRIC_NIO_THREAD_FRAMES_TRANSCEIVED;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.NETWORKING_METRIC_NIO_THREAD_IDLE_TIME_MILLIS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.NETWORKING_METRIC_NIO_THREAD_IO_THREAD_ID;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.NETWORKING_METRIC_NIO_THREAD_PRIORITY_FRAMES_TRANSCEIVED;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.NETWORKING_METRIC_NIO_THREAD_PROCESS_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.NETWORKING_METRIC_NIO_THREAD_SELECTOR_IO_EXCEPTION_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.NETWORKING_METRIC_NIO_THREAD_SELECTOR_REBUILD_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.NETWORKING_METRIC_NIO_THREAD_TASK_QUEUE_SIZE;
import static com.hazelcast.internal.metrics.MetricTarget.MANAGEMENT_CENTER;
import static com.hazelcast.internal.metrics.ProbeUnit.BYTES;
import static com.hazelcast.internal.metrics.ProbeUnit.MS;
import static com.hazelcast.internal.networking.nio.SelectorMode.SELECT_NOW;
import static com.hazelcast.internal.networking.nio.SelectorOptimizer.newSelector;
import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static java.lang.Math.max;
import static java.lang.System.currentTimeMillis;

@ExcludedMetricTargets(MANAGEMENT_CENTER)
public class NioThread extends HazelcastManagedThread implements OperationHostileThread {

    // WARNING: This value has significant effect on idle CPU usage!
    private static final int SELECT_WAIT_TIME_MILLIS
            = Integer.getInteger("hazelcast.io.select.wait.time.millis", 5000);
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
    // this field is set during construction and is meant for the probes so that the NioPipeline can
    // indicate which thread they are currently bound to.
    @Probe(name = NETWORKING_METRIC_NIO_THREAD_IO_THREAD_ID)
    public int id;

    @Probe(name = NETWORKING_METRIC_NIO_THREAD_BYTES_TRANSCEIVED, unit = BYTES)
    volatile long bytesTransceived;
    @Probe(name = NETWORKING_METRIC_NIO_THREAD_FRAMES_TRANSCEIVED)
    volatile long framesTransceived;
    @Probe(name = NETWORKING_METRIC_NIO_THREAD_PRIORITY_FRAMES_TRANSCEIVED)
    volatile long priorityFramesTransceived;
    @Probe(name = NETWORKING_METRIC_NIO_THREAD_PROCESS_COUNT)
    volatile long processCount;

    @Probe(name = NETWORKING_METRIC_NIO_THREAD_TASK_QUEUE_SIZE)
    private final Queue<Runnable> taskQueue = new ConcurrentLinkedQueue<>();
    @Probe(name = NETWORKING_METRIC_NIO_THREAD_EVENT_COUNT)
    private final SwCounter eventCount = newSwCounter();
    @Probe(name = NETWORKING_METRIC_NIO_THREAD_SELECTOR_IO_EXCEPTION_COUNT)
    private final SwCounter selectorIOExceptionCount = newSwCounter();
    @Probe(name = NETWORKING_METRIC_NIO_THREAD_COMPLETED_TASK_COUNT)
    private final SwCounter completedTaskCount = newSwCounter();
    // count number of times the selector was rebuilt (if selectWorkaround is enabled)
    @Probe(name = NETWORKING_METRIC_NIO_THREAD_SELECTOR_REBUILD_COUNT)
    private final SwCounter selectorRebuildCount = newSwCounter();

    private final ILogger logger;

    private Selector selector;

    private final ChannelErrorHandler errorHandler;

    private final SelectorMode selectMode;

    private final IdleStrategy idleStrategy;

    // last time select unblocked with some keys selected
    private volatile long lastSelectTimeMs;

    private volatile boolean stop;

    // set to true while testing
    private boolean selectorWorkaroundTest;

    public NioThread(String threadName,
                     ILogger logger,
                     ChannelErrorHandler errorHandler) {
        this(threadName, logger, errorHandler, SelectorMode.SELECT, null);
    }

    public NioThread(String threadName,
                     ILogger logger,
                     ChannelErrorHandler errorHandler,
                     SelectorMode selectMode,
                     IdleStrategy idleStrategy) {
        this(threadName, logger, errorHandler, selectMode, newSelector(logger), idleStrategy);
    }

    public NioThread(String threadName,
                     ILogger logger,
                     ChannelErrorHandler errorHandler,
                     SelectorMode selectMode,
                     Selector selector,
                     IdleStrategy idleStrategy) {
        super(threadName);
        this.logger = logger;
        this.selectMode = selectMode;
        this.errorHandler = errorHandler;
        this.selector = selector;
        this.selectorWorkaroundTest = false;
        this.idleStrategy = idleStrategy;
    }

    void setSelectorWorkaroundTest(boolean selectorWorkaroundTest) {
        this.selectorWorkaroundTest = selectorWorkaroundTest;
    }

    public long bytesTransceived() {
        return bytesTransceived;
    }

    public long framesTransceived() {
        return framesTransceived;
    }

    public long priorityFramesTransceived() {
        return priorityFramesTransceived;
    }

    public long handleCount() {
        return processCount;
    }

    public long eventCount() {
        return eventCount.get();
    }

    public long completedTaskCount() {
        return completedTaskCount.get();
    }

    /**
     * Gets the Selector
     *
     * @return the Selector
     */
    public Selector getSelector() {
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
     * A probe that measure how long this NioThread has not received any events.
     *
     * @return the idle time in ms.
     */
    @Probe(name = NETWORKING_METRIC_NIO_THREAD_IDLE_TIME_MILLIS, unit = MS)
    private long idleTimeMillis() {
        return max(currentTimeMillis() - lastSelectTimeMs, 0);
    }

    /**
     * Adds a task to this NioThread without notifying the thread.
     *
     * @param task the task to add
     * @throws NullPointerException if task is null
     */
    public void addTask(Runnable task) {
        taskQueue.add(task);
    }

    /**
     * Adds a task to be executed by the NioThread and wakes up the selector so that it will
     * eventually pick up the task.
     *
     * @param task the task to add.
     * @throws NullPointerException if task is null
     */
    public void addTaskAndWakeup(Runnable task) {
        taskQueue.add(task);
        if (selectMode != SELECT_NOW) {
            selector.wakeup();
        }
    }

    @Override
    public void executeRun() {
        // This outer loop is a bit complex but it takes care of a lot of stuff:
        // * it calls runSelectNowLoop or runSelectLoop based on selectNow enabled or not.
        // * handles backoff and retrying in case if io exception is thrown
        // * it takes care of other exception handling.
        //
        // The idea about this approach is that the runSelectNowLoop and runSelectLoop are
        // as clean as possible and don't contain any logic that isn't happening on the happy-path.
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
        } catch (Throwable e) {
            errorHandler.onError(null, e);
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
        while (!stop) {
            processTaskQueue();

            int selectedKeys = selector.select(SELECT_WAIT_TIME_MILLIS);
            if (selectedKeys > 0) {
                processSelectionKeys();
            }
        }
    }

    private void selectLoopWithFix() throws IOException {
        int idleCount = 0;
        while (!stop) {
            processTaskQueue();

            long before = currentTimeMillis();
            int selectedKeys = selector.select(SELECT_WAIT_TIME_MILLIS);
            if (selectedKeys > 0) {
                idleCount = 0;
                processSelectionKeys();
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
        long idleRound = 0;
        while (!stop) {
            boolean tasksProcessed = processTaskQueue();

            int selectedKeys = selector.selectNow();

            if (selectedKeys > 0) {
                processSelectionKeys();
                idleRound = 0;
            } else if (tasksProcessed) {
                idleRound = 0;
            } else if (idleStrategy != null) {
                idleRound++;
                idleStrategy.idle(idleRound);
            }
        }
    }

    private boolean processTaskQueue() {
        boolean tasksProcessed = false;
        while (!stop) {
            Runnable task = taskQueue.poll();
            if (task == null) {
                break;
            }
            task.run();
            completedTaskCount.inc();
            tasksProcessed = true;
        }
        return tasksProcessed;
    }

    private void processSelectionKeys() {
        lastSelectTimeMs = currentTimeMillis();
        Iterator<SelectionKey> it = selector.selectedKeys().iterator();
        while (it.hasNext()) {
            SelectionKey sk = it.next();
            it.remove();
            processSelectionKey(sk);
        }
    }

    private void processSelectionKey(SelectionKey sk) {
        NioPipeline pipeline = (NioPipeline) sk.attachment();
        try {
            if (!sk.isValid()) {
                // if the selectionKey isn't valid, we throw this exception to feedback the situation into the pipeline.onFailure
                throw new CancelledKeyException();
            }

            // we don't need to check for sk.isReadable/sk.isWritable since the pipeline has only registered
            // for events it can handle.
            eventCount.inc();
            pipeline.process();
        } catch (Throwable t) {
             pipeline.onError(t);
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

    public void shutdown() {
        stop = true;
        taskQueue.clear();
        interrupt();
    }

    // this method is always invoked in this thread
    // after we have blocked for selector.select in #runSelectLoopWithSelectorFix
    private void rebuildSelector() {
        selectorRebuildCount.inc();
        Selector newSelector = newSelector(logger);
        Selector oldSelector = this.selector;

        // reset each pipeline's selectionKey, cancel the old keys
        for (SelectionKey key : oldSelector.keys()) {
            NioPipeline pipeline = (NioPipeline) key.attachment();

            try {
                int ops = key.interestOps();
                pipeline.initSelectionKey(newSelector, ops);
            } catch (ClosedChannelException e) {
                logger.info("Channel was closed while trying to register with new selector.");
            } catch (CancelledKeyException e) {
                // a CancelledKeyException may be thrown in key.interestOps
                // in this case, since the key is already cancelled, just do nothing
                ignore(e);
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
}
