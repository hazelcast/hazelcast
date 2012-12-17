/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util;

import java.util.Collection;
import java.util.concurrent.*;

/**
 * @mdogan 12/17/12
 */
public class FastExecutor implements Executor {

    private final BacklogDetector backlogDetector = new BacklogDetector();
    private final BlockingQueue<Task> queue = new LinkedBlockingQueue<Task>();
    private final Collection<Thread> threads = new ConcurrentHashSet<Thread>();
    private final ThreadFactory threadFactory;
    private volatile boolean live = true;

    public FastExecutor(int coreSize, ThreadFactory threadFactory) {
        this.threadFactory = threadFactory;
        Thread t = threadFactory.newThread(backlogDetector);
        threads.add(t);
        t.start();
        for (int i = 0; i < coreSize; i++) {
            addThread();
        }
    }

    public void execute(Runnable command) {
        if (!live) throw new RejectedExecutionException("Executor has been shutdown!");
        queue.offer(new Task(command));
    }

    public void shutdown() {
        live = false;
        for (Thread thread : threads) {
            thread.interrupt();
        }
        queue.clear();
        threads.clear();
    }

    private void addThread() {
        final Worker worker = new Worker();
        final Thread thread = threadFactory.newThread(worker);
        threads.add(thread);
        thread.start();
    }

    private class Worker implements Runnable {
        public void run() {
            final Thread thread = Thread.currentThread();
            while (!thread.isInterrupted() && live) {
                try {
                    Task task = queue.take();
                    task.run();
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    }

    private class BacklogDetector implements Runnable {
        final long diff = TimeUnit.SECONDS.toMillis(1);

        public void run() {
            final Thread thread = Thread.currentThread();
            while (!thread.isInterrupted() && live) {
                final Task task = queue.peek();
                if (task != null) {
                    if (task.creationTime + diff < System.currentTimeMillis()) {
                        addThread();
                    }
                }
                try {
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }
    }

    private class Task implements Runnable {
        final long creationTime = System.currentTimeMillis();
        final Runnable task;

        private Task(Runnable task) {
            this.task = task;
        }

        public void run() {
            task.run();
        }
    }
}
