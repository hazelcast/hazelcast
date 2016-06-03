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

package com.hazelcast.jet.impl.executor.processor;

import com.hazelcast.jet.impl.actor.SleepingStrategy;
import com.hazelcast.jet.impl.actor.strategy.AdaptiveSleepingStrategy;
import com.hazelcast.jet.impl.util.SettableFuture;
import com.hazelcast.jet.impl.executor.AbstractExecutor;
import com.hazelcast.jet.impl.executor.Payload;
import com.hazelcast.jet.impl.executor.Task;
import com.hazelcast.jet.impl.executor.WorkingProcessor;
import com.hazelcast.jet.impl.container.task.DefaultContainerTask;
import com.hazelcast.logging.ILogger;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.util.Preconditions.checkTrue;

public abstract class AbstractExecutorProcessor<E extends AbstractExecutor>
        implements WorkingProcessor {
    protected final ILogger logger;
    protected final SleepingStrategy sleepingStrategy;
    protected final List<Task> tasks = new ArrayList<Task>();
    protected final AtomicInteger workingTaskCount = new AtomicInteger(0);
    protected final SettableFuture<Boolean> shutdownFuture = SettableFuture.create();
    protected final BlockingQueue<Boolean> lockingQueue = new ArrayBlockingQueue<Boolean>(1);
    protected final Payload payload = new Payload() {
        private boolean produced;

        @Override
        public void set(boolean produced) {
            this.produced = produced;
        }

        @Override
        public boolean produced() {
            return produced;
        }
    };
    protected final E taskExecutor;
    protected final Queue<Task> incomingTasks = new ConcurrentLinkedQueue<Task>();
    protected boolean started;
    protected volatile boolean shutdown;
    protected volatile Thread workingThread;
    protected volatile boolean hasIncoming;

    public AbstractExecutorProcessor(int threadNum,
                                     ILogger logger,
                                     E taskExecutor) {
        checkTrue(threadNum >= 0, "threadNum must be positive");

        this.logger = logger;
        this.taskExecutor = taskExecutor;
        this.sleepingStrategy = new AdaptiveSleepingStrategy();
    }

    public Future<Boolean> shutdown() {
        this.shutdown = true;

        wakeUp();

        if ((this.workingThread != null)) {
            this.workingThread.interrupt();
        }

        return this.shutdownFuture;
    }

    protected void await() throws InterruptedException {
        this.lockingQueue.take();
    }

    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    public void start() {
        if (!this.started) {
            this.started = true;
            this.tasks.clear();
            this.incomingTasks.clear();
            this.workingTaskCount.set(0);
            this.lockingQueue.offer(true);
        }
    }

    @Override
    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    public void wakeUp() {
        this.lockingQueue.offer(true);
    }

    protected void checkIncoming() {
        if (this.hasIncoming) {
            this.hasIncoming = false;
            readIncomingTasks(this.incomingTasks, true);
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

            this.tasks.add(task);
            this.workingTaskCount.incrementAndGet();

            if (task instanceof DefaultContainerTask) {
                System.out.println("Incoming=" + task.getClass() + " size=" + this.tasks.size()
                                + " idx=" + this + " wtc=" + this.workingTaskCount.get()
                );
            }
        } while (true);
    }

    protected boolean execute() throws Exception {
        checkIncoming();

        boolean payLoad = false;
        int idx = 0;

        while (idx < this.tasks.size()) {
            Task task = this.tasks.get(idx);

            boolean activeTask = task.executeTask(this.payload);
            payLoad = payLoad || this.payload.produced();

            if (!activeTask) {
                this.workingTaskCount.decrementAndGet();
                this.tasks.remove(idx);
                onTaskDeactivation();

                if (task instanceof DefaultContainerTask) {
                    System.out.println(
                            "Task removed " + tasks.size() + " this.workingTaskCount="
                                    + this.workingTaskCount.get() + " " + this
                    );
                }
            } else {
                idx++;
            }
        }

        return payLoad;
    }

    protected void onTaskDeactivation() {

    }

    public void run() {
        this.workingThread = Thread.currentThread();
        boolean wasPayLoad = false;

        try {
            while (!this.shutdown) {
                boolean payLoad = false;

                try {
                    payLoad = execute();

                    if (!payLoad) {
                        if (this.shutdown) {
                            break;
                        }

                        this.sleepingStrategy.await(wasPayLoad);

                        if (tasks.size() == 0) {
                            if (this.tasks.size() > 0) {
                                System.out.println(
                                        "size=" + this.tasks.size()
                                );
                            }

                            checkExecutorActivity();
                        }
                    }
                } catch (Throwable e) {
                    e.printStackTrace(System.out);
                    this.logger.warning(e.getMessage(), e);
                }

                wasPayLoad = payLoad;
            }
        } finally {
            this.tasks.clear();
            this.shutdownFuture.set(true);
        }
    }

    @Override
    public void consumeTask(Task task) {
        this.incomingTasks.offer(task);
        this.hasIncoming = true;
    }

    protected void checkExecutorActivity() {

    }

    @Override
    public int getWorkingTaskCount() {
        return this.workingTaskCount.get();
    }
}
