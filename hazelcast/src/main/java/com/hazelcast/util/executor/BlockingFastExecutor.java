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

package com.hazelcast.util.executor;

import com.hazelcast.util.Clock;

import java.util.concurrent.*;

/**
 * @mdogan 12/17/12
 */
public class BlockingFastExecutor extends FastExecutorSupport implements FastExecutor {

    private final BlockingQueue<WorkerTask> queue;

    public BlockingFastExecutor(int coreThreadSize, String namePrefix, ThreadFactory threadFactory) {
        this(coreThreadSize, coreThreadSize * 10, coreThreadSize * (1 << 16),
                500L, namePrefix, threadFactory, TimeUnit.SECONDS.toMillis(60), false);
    }

    public BlockingFastExecutor(int coreThreadSize, int maxThreadSize, int queueCapacity,
                                long backlogIntervalInMillis, String namePrefix, ThreadFactory threadFactory,
                                long keepAliveMillis, boolean allowCoreThreadTimeout) {

        super(coreThreadSize, maxThreadSize, queueCapacity, backlogIntervalInMillis, namePrefix,
                threadFactory, keepAliveMillis, allowCoreThreadTimeout);
        this.queue = new LinkedBlockingQueue<WorkerTask>(queueCapacity > 0 ? queueCapacity : Integer.MAX_VALUE);
    }

    protected boolean offerTask(Runnable command) {
        try {
            if (!queue.offer(new WorkerTask(command), backlogInterval, TimeUnit.MILLISECONDS)) {
                throw new RejectedExecutionException("Executor reached to max capacity!");
            }
        } catch (InterruptedException e) {
            throw new RejectedExecutionException(e);
        }
        return true;
    }

    protected void onShutdown() {
        queue.clear();
    }

    protected Runnable createBacklogDetector() {
        return new BacklogDetector();
    }

    protected Runnable createWorker() {
        return new Worker();
    }

    private class Worker implements Runnable {
        public void run() {
            final Thread currentThread = Thread.currentThread();
            final boolean take = keepAliveMillis <= 0 || keepAliveMillis == Long.MAX_VALUE;
            final long timeout = keepAliveMillis;
            while (!currentThread.isInterrupted() && isLive()) {
                try {
                    final WorkerTask task = take ? queue.take() : queue.poll(timeout, TimeUnit.MILLISECONDS);
                    if (task != null) {
                        task.run();
                    } else {
                        if (removeWorker(currentThread)) {
                            return;
                        }
                    }
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    }

    private class BacklogDetector implements Runnable {

        public void run() {
            long currentBacklogInterval = backlogInterval;
            final Thread thread = Thread.currentThread();
            int k = 0;
            while (!thread.isInterrupted() && isLive()) {
                long sleep = 100;
                final WorkerTask task = queue.peek();
                if (task != null) {
                    if (task.creationTime + currentBacklogInterval < Clock.currentTimeMillis()) {
                        addWorkerIfUnderMaxSize();
                    }
                }
                try {
                    Thread.sleep(sleep);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }
    }

}
