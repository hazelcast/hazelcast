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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @mdogan 12/17/12
 */
abstract class FastExecutorSupport implements FastExecutor {

    private final ThreadFactory threadFactory;
    private final Collection<Thread> threads = Collections.newSetFromMap(new ConcurrentHashMap<Thread, Boolean>(16, 0.75f, 1));
    private final Thread backlogDetector;

    protected final int coreThreadSize;
    protected final int maxThreadSize;
    protected final int queueCapacity;
    protected final long backlogInterval;
    protected final long keepAliveMillis;
    protected final boolean allowCoreThreadTimeout;
    protected final Lock lock = new ReentrantLock();

    private volatile WorkerLifecycleInterceptor interceptor;
    private volatile int activeThreadCount;
    private volatile boolean live = true;

    public FastExecutorSupport(int coreThreadSize, int maxThreadSize, int queueCapacity,
                               long backlogIntervalInMillis, String namePrefix, ThreadFactory threadFactory,
                               long keepAliveMillis, boolean allowCoreThreadTimeout) {
        this.queueCapacity = queueCapacity;
        this.threadFactory = threadFactory;
        this.keepAliveMillis = keepAliveMillis;
        this.coreThreadSize = coreThreadSize;
        this.maxThreadSize = maxThreadSize;
        this.backlogInterval = backlogIntervalInMillis;
        this.allowCoreThreadTimeout = allowCoreThreadTimeout;
        this.backlogDetector = new Thread(createBacklogDetector(), namePrefix + "backlog");
    }

    public final void execute(Runnable command) {
        if (!isLive()) throw new RejectedExecutionException("Executor has been shutdown!");
        if (offerTask(command)) {
            if (getActiveThreadCount() < coreThreadSize) {
                addWorkerIfUnderCoreSize();
            }
        }
    }

    protected abstract boolean offerTask(Runnable command);

    protected abstract Runnable createBacklogDetector();

    protected abstract Runnable createWorker();

    protected abstract void onShutdown();

    public final void shutdown() {
        lock.lock();
        try {
            live = false;
            backlogDetector.interrupt();
            for (Thread thread : threads) {
                thread.interrupt();
            }
            onShutdown();
            threads.clear();
        } finally {
            lock.unlock();
        }
    }

    protected final void addWorkerIfUnderCoreSize() {
        final int maxSize = coreThreadSize;
        if (activeThreadCount < maxSize) {
            addWorker(maxSize);
        }
    }

    protected final void addWorkerIfUnderMaxSize() {
        final int maxSize = maxThreadSize;
        if (activeThreadCount < maxSize) {
            addWorker(maxSize);
        }
    }

    private void addWorker(final int maxSize) {
        lock.lock();
        try {
            if (activeThreadCount < maxSize && live) {
                final WorkerLifecycleInterceptor workerInterceptor = interceptor;
                if (workerInterceptor != null) {
                    workerInterceptor.beforeWorkerStart();
                }
                if (backlogDetector.getState() == Thread.State.NEW) {
                    backlogDetector.start();
                }
                final Runnable worker = createWorker();
                final Thread thread = threadFactory.newThread(worker);
                thread.start();
                activeThreadCount++;
                threads.add(thread);
            }
        } finally {
            lock.unlock();
        }
    }

    protected final boolean removeWorker(Thread workerThread) throws InterruptedException {
        lock.lockInterruptibly();
        try {
            if (activeThreadCount > coreThreadSize || allowCoreThreadTimeout) {
                threads.remove(workerThread);
                activeThreadCount--;
                final WorkerLifecycleInterceptor workerInterceptor = interceptor;
                if (workerInterceptor != null) {
                    workerInterceptor.afterWorkerTerminate();
                }
                return true;
            }
        } finally {
            lock.unlock();
        }
        return false;
    }

    final class WorkerTask implements Runnable {
        final long creationTime = Clock.currentTimeMillis();
        final Runnable task;

        WorkerTask(Runnable task) {
            this.task = task;
        }

        public void run() {
            task.run();
        }
    }

    protected final boolean isLive() {
        return live;
    }

    public final void setInterceptor(WorkerLifecycleInterceptor interceptor) {
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
}
