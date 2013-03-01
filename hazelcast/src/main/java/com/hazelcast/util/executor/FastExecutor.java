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

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @mdogan 12/17/12
 */
public class FastExecutor implements Executor {

    private final BlockingQueue<WorkerTask> queue;
    private final Collection<Thread> threads = Collections.newSetFromMap(new ConcurrentHashMap<Thread, Boolean>());
    private final ThreadFactory threadFactory;
    private final int coreThreadSize;
    private final int maxThreadSize;
    private final long backlogInterval;
    private final long keepAliveMillis;
    private final boolean allowCoreThreadTimeout;
    private final Lock lock = new ReentrantLock();
    private final NewThreadInterceptor interceptor = null; // TODO: for future use.
    private volatile int activeThreadCount;
    private volatile boolean live = true;

    public FastExecutor(int coreThreadSize, String namePrefix, ThreadFactory threadFactory) {
        this(coreThreadSize, coreThreadSize * 20, Math.max(Integer.MAX_VALUE, coreThreadSize * (1 << 16)),
                500L, namePrefix, threadFactory, TimeUnit.MINUTES.toMillis(5), false, true);
    }

    public FastExecutor(int coreThreadSize, int maxThreadSize, int queueCapacity,
                        long backlogIntervalInMillis, String namePrefix, ThreadFactory threadFactory,
                        long keepAliveMillis, boolean allowCoreThreadTimeout, boolean startCoreThreads) {
        this.threadFactory = threadFactory;
        this.keepAliveMillis = keepAliveMillis;
        this.coreThreadSize = coreThreadSize;
        this.maxThreadSize = maxThreadSize;
        this.backlogInterval = backlogIntervalInMillis;
        this.allowCoreThreadTimeout = allowCoreThreadTimeout;
        this.queue = new LinkedBlockingQueue<WorkerTask>(queueCapacity);

        Thread t = new Thread(new BacklogDetector(), namePrefix + "backlog");
        threads.add(t);
        if (startCoreThreads) {
            t.start();
        }
        for (int i = 0; i < coreThreadSize; i++) {
            addThread(startCoreThreads);
        }
    }

    public void execute(Runnable command) {
        if (!live) throw new RejectedExecutionException("Executor has been shutdown!");
        try {
            if (!queue.offer(new WorkerTask(command), backlogInterval, TimeUnit.MILLISECONDS)) {
                throw new RejectedExecutionException("Executor reached to max capacity!");
            }
        } catch (InterruptedException e) {
            throw new RejectedExecutionException(e);
        }
    }

    public void start() {
        for (Thread thread : threads) {
            if (thread.getState() == Thread.State.NEW) {
                thread.start();
            }
        }
    }

    public void shutdown() {
        live = false;
        for (Thread thread : threads) {
            thread.interrupt();
        }
        queue.clear();
        threads.clear();
    }

    private void addThread(boolean start) {
        lock.lock();
        try {
            final Worker worker = new Worker();
            final Thread thread = threadFactory.newThread(worker);
            if (start) {
                thread.start();
            }
            activeThreadCount++;
            threads.add(thread);
        } finally {
            lock.unlock();
        }
    }

    private class Worker implements Runnable {
        public void run() {
            final Thread currentThread = Thread.currentThread();
            final boolean take = keepAliveMillis <= 0 || keepAliveMillis == Long.MAX_VALUE;
            final long timeout = keepAliveMillis;
            while (!currentThread.isInterrupted() && live) {
                try {
                    final WorkerTask task = take ? queue.take() : queue.poll(timeout, TimeUnit.MILLISECONDS);
                    if (task != null) {
                        task.run();
                    } else {
                        final int activeCount = activeThreadCount;
                        if (activeCount > coreThreadSize || allowCoreThreadTimeout) {
                            lock.lockInterruptibly();
                            try {
                                threads.remove(currentThread);
                                activeThreadCount--;
                                return;
                            } finally {
                                lock.unlock();
                            }
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
            final NewThreadInterceptor threadInterceptor = interceptor;
            long currentBacklogInterval = backlogInterval;
            final Thread thread = Thread.currentThread();
            while (!thread.isInterrupted() && live) {
                long sleep = 100;
                final WorkerTask task = queue.peek();
                if (task != null) {
                    if (task.creationTime + currentBacklogInterval < Clock.currentTimeMillis()) {
                        if (activeThreadCount < maxThreadSize) {
                            if (threadInterceptor != null) {
                                threadInterceptor.beforeNewThread();
                            }
                            addThread(true);
                            // increase backlog check interval on each thread creation
                            currentBacklogInterval += 100;
                        } else {
                            sleep = 1000;
                        }
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

    private class WorkerTask implements Runnable {
        final long creationTime = Clock.currentTimeMillis();
        final Runnable task;

        private WorkerTask(Runnable task) {
            this.task = task;
        }

        public void run() {
            task.run();
        }
    }

    public interface NewThreadInterceptor {
        boolean beforeNewThread();
    }
}
