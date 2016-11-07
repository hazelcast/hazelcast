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

package com.hazelcast.jet.impl.executor;

import com.hazelcast.jet.impl.runtime.task.VertexTask;
import com.hazelcast.jet.impl.util.BooleanHolder;
import com.hazelcast.jet.impl.util.SettableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.util.concurrent.IdleStrategy;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class Worker implements Runnable {
    protected static final long MAX_PARK_PERIOD_NS = 1_000_000L;

    protected final ILogger logger;
    protected final IdleStrategy idleStrategy;
    protected final List<Task> tasks = new ArrayList<>();
    protected final AtomicInteger workingTaskCount = new AtomicInteger(0);
    protected final SettableFuture<Boolean> shutdownFuture = SettableFuture.create();
    protected final BlockingQueue<Boolean> lockingQueue = new ArrayBlockingQueue<>(1);
    protected final BooleanHolder didWorkHolder = new BooleanHolder();

    protected final Queue<Task> incomingTasks = new ConcurrentLinkedQueue<>();
    protected boolean started;
    protected volatile boolean shutdown;
    protected volatile Thread workingThread;
    protected volatile boolean hasIncoming;

    public Worker(ILogger logger) {
        this.logger = logger;
        idleStrategy = new BackoffIdleStrategy(0, 0, 1L, MAX_PARK_PERIOD_NS);
    }

    /**
     * Asynchronously shutdown processor
     *
     * @return awaiting Future object
     */
    public Future<Boolean> shutdown() {
        shutdown = true;

        wakeUp();

        if ((workingThread != null)) {
            workingThread.interrupt();
        }

        return shutdownFuture;
    }

    protected void await() throws InterruptedException {
        lockingQueue.take();
    }

    /**
     * Start processor's execution
     */
    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    public void start() {
        if (!started) {
            started = true;
            tasks.clear();
            incomingTasks.clear();
            workingTaskCount.set(0);
            lockingQueue.offer(true);
        }
    }

    /**
     * Notify processor to wakeUp
     */
    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    public void wakeUp() {
        lockingQueue.offer(true);
    }

    protected void checkIncoming() {
        if (hasIncoming) {
            hasIncoming = false;
            readIncomingTasks(incomingTasks, true);
        }
    }

    protected void readIncomingTasks(Queue<Task> incomingTask,
                                     boolean initTask) {
        do {
            Task task = incomingTask.poll();

            if (task == null) {
                break;
            }

            if (initTask) {
                task.beforeProcessing();
            }

            tasks.add(task);
            workingTaskCount.incrementAndGet();

            if (logger.isFinestEnabled() && task instanceof VertexTask) {
                logger.finest("Incoming=" + task.getClass() + " size=" + tasks.size()
                        + " idx=" + this + " wtc=" + workingTaskCount.get()
                );
            }
        } while (true);
    }

    protected boolean execute() throws Exception {
        checkIncoming();

        boolean didWork = false;
        int idx = 0;

        while (idx < tasks.size()) {
            Task task = tasks.get(idx);

            boolean isActive = task.execute(didWorkHolder);
            didWork = didWork || didWorkHolder.get();

            if (!isActive) {
                workingTaskCount.decrementAndGet();
                tasks.remove(idx);
                onTaskDeactivation();

                if (task instanceof VertexTask) {
                    logger.fine("Task removed " + tasks.size() + " workingTaskCount="
                            + workingTaskCount.get() + " " + this);
                }
            } else {
                idx++;
            }
        }

        return didWork;
    }

    protected void onTaskDeactivation() {

    }

    @Override
    public void run() {
        workingThread = Thread.currentThread();
        int idleCount = 0;
        try {
            while (!shutdown) {
                boolean didWork = false;
                try {
                    didWork = execute();
                    if (!didWork && !shutdown) {
                        idleStrategy.idle(idleCount);
                        if (tasks.size() == 0) {
                            checkExecutorActivity();
                        }
                    }
                } catch (Throwable e) {
                    logger.severe("Error executing task", e);
                }
                idleCount = didWork ? 0 : idleCount + 1;
            }
        } finally {
            tasks.clear();
            shutdownFuture.set(true);
        }
    }

    /**
     * Consume task
     *
     * @param task corresponding taks
     */
    public void consumeTask(Task task) {
        incomingTasks.offer(task);
        hasIncoming = true;
    }

    protected void checkExecutorActivity() {

    }

    /**
     * @return number of tasks inside vertex runner
     */
    public int getWorkingTaskCount() {
        return workingTaskCount.get();
    }


}
