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

import com.hazelcast.instance.Node;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.instance.ThreadContext;
import com.hazelcast.logging.ILogger;

import java.util.Collection;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

/**
 * @mdogan 12/17/12
 */
public class FastExecutor implements Executor {

    private static final AtomicInteger ID = new AtomicInteger();

    private final ILogger logger;
    private final Node node;
    private final BacklogDetector backlogDetector;
    private final BlockingQueue<Task> queue = new LinkedBlockingQueue<Task>();
    private final Collection<Thread> threads = new ConcurrentHashSet<Thread>();
    private volatile boolean live = true;

    public FastExecutor(Node node, int coreSize) {
        this.node = node;
        logger = node.getLogger(FastExecutor.class.getName());
        backlogDetector = new BacklogDetector(node.threadGroup, getThreadName(node));
        backlogDetector.start();
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
        backlogDetector.interrupt();
        for (Thread thread : threads) {
            thread.interrupt();
        }
        queue.clear();
        threads.clear();
    }

    private String getThreadName(Node node) {
        return node.getThreadPoolNamePrefix("fast-executor") + ID.getAndIncrement();
    }

    private void addThread() {
        final Worker worker = new Worker(node.threadGroup, getThreadName(node));
        worker.start();
        threads.add(worker);
        logger.log(Level.INFO, "Added new thread, total: " + threads.size());
    }

    private class Worker extends Thread {
        private Worker(ThreadGroup group, String name) {
            super(group, name);
        }

        public void run() {
            try {
                ThreadContext.get().setCurrentInstance(node.hazelcastInstance);
                setContextClassLoader(node.getConfig().getClassLoader());

                while (!isInterrupted() && live) {
                    try {
                        Task task = queue.take();
                        task.run();
                    } catch (InterruptedException e) {
                        break;
                    } catch (Exception e) {
                        logger.log(Level.WARNING, e.getMessage(), e);
                    } catch (OutOfMemoryError e) {
                        OutOfMemoryErrorDispatcher.onOutOfMemory(e);
                    }
                }
            } finally {
                try {
                    ThreadContext.shutdown(this);
                } catch (Throwable e) {
                    logger.log(Level.SEVERE, e.getMessage(), e);
                }
            }
        }
    }

    private class BacklogDetector extends Thread {
        final long diff = TimeUnit.SECONDS.toMillis(1);

        private BacklogDetector(ThreadGroup group, String name) {
            super(group, name);
        }

        public void run() {
            while (!isInterrupted() && live) {
                final Task task = queue.peek();
                if (task != null) {
                    if (task.creationTime + diff < System.currentTimeMillis()) {
                        addThread();
                    }
                }
                try {
                    sleep(5);
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
