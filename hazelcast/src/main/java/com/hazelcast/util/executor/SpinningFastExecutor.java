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

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.LockSupport;

/**
 * @mdogan 12/17/12
 */
public class SpinningFastExecutor extends FastExecutorSupport implements FastExecutor {

    private final Queue<WorkerTask> queue;
    private final Condition signalWorker = lock.newCondition();
    private final AtomicInteger spinningThreads = new AtomicInteger();

    public SpinningFastExecutor(int coreThreadSize, String namePrefix, ThreadFactory threadFactory) {
        this(coreThreadSize, coreThreadSize * 10, coreThreadSize * (1 << 16),
                500L, namePrefix, threadFactory, TimeUnit.SECONDS.toMillis(60), false);
    }

    public SpinningFastExecutor(int coreThreadSize, int maxThreadSize, int queueCapacity,
                                long backlogIntervalInMillis, String namePrefix, ThreadFactory threadFactory,
                                long keepAliveMillis, boolean allowCoreThreadTimeout) {

        super(coreThreadSize, maxThreadSize, queueCapacity, backlogIntervalInMillis, namePrefix,
                threadFactory, keepAliveMillis, allowCoreThreadTimeout);
        this.queue = new ConcurrentLinkedQueue<WorkerTask>();  // TODO: @mm - capacity check!
    }

    protected boolean offerTask(Runnable command) {
        if (!queue.offer(new WorkerTask(command))) {
            throw new RejectedExecutionException("Executor reached to max capacity!");
        }
        if (spinningThreads.get() < coreThreadSize) {
            lock.lock();
            try {
                signalWorker.signal();
            } finally {
                lock.unlock();
            }
        }
        return true;
    }

    protected Runnable createBacklogDetector() {
        return new BacklogDetector();
    }

    protected Runnable createWorker() {
        return new Worker();
    }

    protected void onShutdown() {
        queue.clear();
    }

    private class Worker implements Runnable {
        public void run() {
            final Thread currentThread = Thread.currentThread();
            final long timeout = keepAliveMillis;
            final int park = 100000;
            final int spin = park + 1000;
            WorkerTask task = null;
            while (!currentThread.isInterrupted() && isLive()) {
                spinningThreads.incrementAndGet();
                int c = spin;
                while (c > 0) {
                    task = queue.poll();
                    if (task != null) {
                        task.run();
                        c = spin;
                    } else {
                        if (currentThread.isInterrupted()) {
                            return;
                        }
                        if (c-- < park) {
                            LockSupport.parkNanos(1L);
                        }
                    }
                }

                try {
                    lock.lockInterruptibly();
                    try {
                        spinningThreads.decrementAndGet();
                        signalWorker.await(timeout, TimeUnit.MILLISECONDS);
                        task = queue.poll();
                        if (task == null && removeWorker(currentThread)) {
                            return;
                        }
                    } finally {
                        lock.unlock();
                    }
                } catch (InterruptedException e) {
                    break;
                }
                if (task != null) { // task polled after await!
                    task.run();
                }
            }
        }
    }

    private class BacklogDetector implements Runnable {

        public void run() {
            final long currentBacklogInterval = backlogInterval;
            final long signalInterval = Math.max(currentBacklogInterval / 5, 100);
            final Thread thread = Thread.currentThread();
            int k = 0;
            while (!thread.isInterrupted() && isLive()) {
                long sleep = 100;
                final WorkerTask task = queue.peek();
                if (task != null) {
                    final long now = Clock.currentTimeMillis();
                    if (task.creationTime + currentBacklogInterval < now) {
                        addWorkerIfUnderMaxSize();
                    } else if (task.creationTime + signalInterval < now) {
                        try {
                            lock.lockInterruptibly();
                        } catch (InterruptedException e) {
                            break;
                        }
                        try {
                            signalWorker.signal();
                        } finally {
                            lock.unlock();
                        }
                    }
                }
                try {
                    Thread.sleep(sleep);
                    if (k++ % 300 == 0) {
                        System.err.println("DEBUG: SPINNING -> Current operation thread count-> " + getActiveThreadCount());
                    }
                } catch (InterruptedException e) {
                    return;
                }
            }
        }
    }

}
