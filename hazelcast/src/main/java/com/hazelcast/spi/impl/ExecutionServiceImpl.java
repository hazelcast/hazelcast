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

package com.hazelcast.spi.impl;

import com.hazelcast.core.RuntimeInterruptedException;
import com.hazelcast.executor.ExecutorThreadFactory;
import com.hazelcast.executor.PoolExecutorThreadFactory;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.annotation.PrivateApi;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.logging.Level;

/**
 * @mdogan 12/14/12
 */
final class ExecutionServiceImpl implements ExecutionService {

    private static final int DEFAULT_THREAD_SIZE = 8;

    private final ExecutorService cachedExecutorService;
    private final ScheduledExecutorService scheduledExecutorService;
    private final ILogger logger;

    private final ConcurrentMap<String, ManagedExecutorService> executors = new ConcurrentHashMap<String, ManagedExecutorService>();

    public ExecutionServiceImpl(NodeEngineImpl nodeEngine) {
        Node node = nodeEngine.getNode();
        logger = node.getLogger(ExecutionService.class.getName());
        final ClassLoader classLoader = node.getConfig().getClassLoader();
        final ExecutorThreadFactory threadFactory = new PoolExecutorThreadFactory(node.threadGroup, node.hazelcastInstance,
                node.getThreadPoolNamePrefix("cached"), classLoader);
        cachedExecutorService = new ThreadPoolExecutor(
                1, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(), threadFactory, new RejectedExecutionHandler() {
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                logger.log(Level.FINEST, "Node is shutting down; discarding the task: " + r);
            }
        });
        scheduledExecutorService = Executors.newScheduledThreadPool(2,
                new PoolExecutorThreadFactory(node.threadGroup,
                        node.hazelcastInstance,
                        node.getThreadPoolNamePrefix("scheduled"), classLoader));

        // default executors
        // TODO: configure using ExecutorService config!
        executors.put("system", new ManagedExecutorService("system", 20, 1));
        executors.put("client", new ManagedExecutorService("client", 40, 1));
        executors.put("scheduled", new ManagedExecutorService("scheduled", 10, 1));
    }

    public ExecutorService getExecutorService(String name) {
        ManagedExecutorService executor = executors.get(name);
        if (executor == null) {
            // TODO: configure using ExecutorService config!
            executor = new ManagedExecutorService(name, DEFAULT_THREAD_SIZE, 1);
            ManagedExecutorService current = executors.putIfAbsent(name, executor);
            executor = current == null ? executor : current;
        }
        return executor;
    }

    public void execute(String name, Runnable command) {
        getExecutorService(name).execute(command);
    }

    public Future<?> submit(String name, Runnable task) {
        return getExecutorService(name).submit(task);
    }

    public <T> Future<T> submit(String name, Callable<T> task) {
        return getExecutorService(name).submit(task);
    }

    public void schedule(final Runnable command, long delay, TimeUnit unit) {
        scheduledExecutorService.schedule(new ScheduledRunner(command), delay, unit);
    }

    public void scheduleAtFixedRate(final Runnable command, long initialDelay, long period, TimeUnit unit) {
        scheduledExecutorService.scheduleAtFixedRate(new ScheduledRunner(command), initialDelay, period, unit);
    }

    public void scheduleWithFixedDelay(final Runnable command, long initialDelay, long period, TimeUnit unit) {
        scheduledExecutorService.scheduleWithFixedDelay(new ScheduledRunner(command), initialDelay, period, unit);
    }

    @PrivateApi
    void shutdown() {
        logger.log(Level.FINEST, "Stopping executors...");
        cachedExecutorService.shutdown();
        scheduledExecutorService.shutdownNow();
        try {
            cachedExecutorService.awaitTermination(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.log(Level.FINEST, e.getMessage(), e);
        }
        for (ManagedExecutorService executorService : executors.values()) {
            executorService.destroy();
        }
        executors.clear();
    }

    private class ScheduledRunner implements Runnable {
        private final Runnable runnable;

        private ScheduledRunner(Runnable runnable) {
            this.runnable = runnable;
        }

        public void run() {
            execute("scheduled", runnable);
        }
    }

    private class ManagedExecutorService implements ExecutorService {

        private final String name;

        private final BlockingQueue<Object> controlQ;

        private final int waitTime;

        private ManagedExecutorService(String name, int maxThreadSize, int waitTimeInSeconds) {
            this.name = name;
            this.waitTime = waitTimeInSeconds;
            this.controlQ = new ArrayBlockingQueue<Object>(maxThreadSize);
            for (int i = 0; i < maxThreadSize; i++) {
                controlQ.offer(new Object());
            }
        }

        public void execute(Runnable command) {
            final Object key = leaseKey();
            cachedExecutorService.execute(new ManagedRunnable(command, controlQ, key));
        }

        public <T> Future<T> submit(Callable<T> task) {
            final Object key = leaseKey();
            return cachedExecutorService.submit(new ManagedCallable<T>(task, controlQ, key));
        }

        public <T> Future<T> submit(Runnable task, T result) {
            final Object key = leaseKey();
            return cachedExecutorService.submit(new ManagedRunnable(task, controlQ, key), result);
        }

        public Future<?> submit(Runnable task) {
            final Object key = leaseKey();
            return cachedExecutorService.submit(new ManagedRunnable(task, controlQ, key));
        }

        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
            throw new UnsupportedOperationException();
        }

        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
            throw new UnsupportedOperationException();
        }

        public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
            throw new UnsupportedOperationException();
        }

        public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            throw new UnsupportedOperationException();
        }

        private Object leaseKey() {
            final Object key;
            try {
                key = controlQ.poll(waitTime, TimeUnit.SECONDS);
                if (key == null) {
                    // TODO: improve logging...
                    logger.log(Level.WARNING, "Executor[" + name + "] is overloaded!");
                }
            } catch (InterruptedException e) {
                throw new RuntimeInterruptedException();
            }
            return key;
        }

        public void shutdown() {
            throw new UnsupportedOperationException();
        }

        public List<Runnable> shutdownNow() {
            throw new UnsupportedOperationException();
        }

        public boolean isShutdown() {
            return cachedExecutorService.isShutdown();
        }

        public boolean isTerminated() {
            return cachedExecutorService.isTerminated();
        }

        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return cachedExecutorService.awaitTermination(timeout, unit);
        }

        void destroy() {
            controlQ.clear();
        }
    }

    private class ManagedRunnable implements Runnable {

        private final Runnable runnable;

        private final BlockingQueue<Object> q;

        private final Object key;

        private ManagedRunnable(Runnable runnable, BlockingQueue<Object> q, Object key) {
            this.runnable = runnable;
            this.q = q;
            this.key = key;
        }

        public void run() {
            try {
                runnable.run();
            } finally {
                releaseKey(key);
            }
        }

        private void releaseKey(final Object key) {
            if (key != null) {
                q.offer(key);
            }
        }
    }

    private class ManagedCallable<V> implements Callable<V> {

        private final Callable<V> callable;

        private final BlockingQueue<Object> q;

        private final Object key;

        private ManagedCallable(Callable<V> callable, BlockingQueue<Object> q, Object key) {
            this.callable = callable;
            this.q = q;
            this.key = key;
        }

        public V call() throws Exception {
            try {
                return callable.call();
            } finally {
                releaseKey(key);
            }
        }

        private void releaseKey(final Object key) {
            if (key != null) {
                q.offer(key);
            }
        }
    }

}
