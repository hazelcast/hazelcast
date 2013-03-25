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

    private volatile WorkerLifecycleInterceptor interceptor;
    private volatile int activeThreadCount;
    private volatile boolean live = true;

    public FastExecutor(int coreThreadSize, String namePrefix, ThreadFactory threadFactory) {
        this(coreThreadSize, coreThreadSize * 20, Math.max(Integer.MAX_VALUE, coreThreadSize * (1 << 16)),
                500L, namePrefix, threadFactory, TimeUnit.SECONDS.toMillis(60), false, true);
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
            addWorker(startCoreThreads);
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

    private void addWorker(boolean start) {
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
                        lock.lockInterruptibly();
                        try {
                            if (activeThreadCount > coreThreadSize || allowCoreThreadTimeout) {
                                threads.remove(currentThread);
                                activeThreadCount--;
                                final WorkerLifecycleInterceptor workerInterceptor = interceptor;
                                if (workerInterceptor != null) {
                                    workerInterceptor.afterWorkerTerminate();
                                }
                                return;
                            }
                        } finally {
                            lock.unlock();
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
            while (!thread.isInterrupted() && live) {
                long sleep = 100;
                final WorkerTask task = queue.peek();
                if (task != null) {
                    if (task.creationTime + currentBacklogInterval < Clock.currentTimeMillis()) {
                        if (activeThreadCount < maxThreadSize) {
                            final WorkerLifecycleInterceptor workerInterceptor = interceptor;
                            if (workerInterceptor != null) {
                                workerInterceptor.beforeWorkerStart();
                            }
                            addWorker(true);
                        }
                    }
                }
                try {
                    Thread.sleep(sleep);
                    if (k++ % 300 == 0) {
                        System.err.println("DEBUG: Current operation thread count-> " + activeThreadCount);
                    }
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

    public void setInterceptor(WorkerLifecycleInterceptor interceptor) {
        this.interceptor = interceptor;
    }

    public int getCoreThreadSize() {
        return coreThreadSize;
    }

    public int getMaxThreadSize() {
        return maxThreadSize;
    }

    public long getKeepAliveMillis() {
        return keepAliveMillis;
    }

    public int getActiveThreadCount() {
        return activeThreadCount;
    }

    public interface WorkerLifecycleInterceptor {
        void beforeWorkerStart();
        void afterWorkerTerminate();
    }
}
