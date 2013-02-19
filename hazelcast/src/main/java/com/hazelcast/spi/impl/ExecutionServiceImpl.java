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

package com.hazelcast.spi.impl;

import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.executor.ManagedExecutorService;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ExecutorThreadFactory;
import com.hazelcast.util.PoolExecutorThreadFactory;

import java.util.concurrent.*;
import java.util.logging.Level;

/**
 * @mdogan 12/14/12
 */
public final class ExecutionServiceImpl implements ExecutionService {

    private final NodeEngineImpl nodeEngine;
    private final ExecutorService cachedExecutorService;
    private final ScheduledExecutorService scheduledExecutorService;
    private final ILogger logger;

    private final ConcurrentMap<String, ExecutorService> executors = new ConcurrentHashMap<String, ExecutorService>();

    public ExecutionServiceImpl(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        Node node = nodeEngine.getNode();
        logger = node.getLogger(ExecutionService.class.getName());
        final ClassLoader classLoader = node.getConfig().getClassLoader();
        final ExecutorThreadFactory threadFactory = new PoolExecutorThreadFactory(node.threadGroup,
                node.getThreadPoolNamePrefix("cached"), classLoader);

        cachedExecutorService = new ThreadPoolExecutor(
                5, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(), threadFactory, new RejectedExecutionHandler() {
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                logger.log(Level.FINEST, "Node is shutting down; discarding the task: " + r);
            }
        });

        scheduledExecutorService = Executors.newScheduledThreadPool(2,
                new PoolExecutorThreadFactory(node.threadGroup,
                        node.getThreadPoolNamePrefix("scheduled"), classLoader));

        // default executors
        register("hz:system", 30, 0);
        register("hz:client", 40, 0);
        register("hz:scheduled", 10, 0);
    }

    ExecutorService register(String name, int maxThreadSize, int queueSize) {
        final ManagedExecutorService executor = new ManagedExecutorService(name, cachedExecutorService, maxThreadSize, queueSize);
        if (executors.putIfAbsent(name, executor) != null) {
            throw new IllegalArgumentException();
        }
        return executor;
    }

    private final ConcurrencyUtil.ConstructorFunction<String, ExecutorService> constructor =
            new ConcurrencyUtil.ConstructorFunction<String, ExecutorService>() {
                public ManagedExecutorService createNew(String name) {
                    final ExecutorConfig cfg = nodeEngine.getConfig().getExecutorConfig(name);
                    return new ManagedExecutorService(name, cachedExecutorService, cfg.getPoolSize(), cfg.getQueueCapacity());
                }
            };

    public ExecutorService getExecutor(String name) {
        return ConcurrencyUtil.getOrPutIfAbsent(executors, name, constructor);
    }

    public void execute(String name, Runnable command) {
        getExecutor(name).execute(command);
    }

    public Future<?> submit(String name, Runnable task) {
        return getExecutor(name).submit(task);
    }

    public <T> Future<T> submit(String name, Callable<T> task) {
        return getExecutor(name).submit(task);
    }

    public ScheduledFuture<?> schedule(final Runnable command, long delay, TimeUnit unit) {
        return scheduledExecutorService.schedule(new ScheduledRunner(command), delay, unit);
    }

    public ScheduledFuture<?> scheduleAtFixedRate(final Runnable command, long initialDelay, long period, TimeUnit unit) {
        return scheduledExecutorService.scheduleAtFixedRate(new ScheduledRunner(command), initialDelay, period, unit);
    }

    public ScheduledFuture<?> scheduleWithFixedDelay(final Runnable command, long initialDelay, long period, TimeUnit unit) {
        return scheduledExecutorService.scheduleWithFixedDelay(new ScheduledRunner(command), initialDelay, period, unit);
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
        for (ExecutorService executorService : executors.values()) {
            executorService.shutdown();
        }
        executors.clear();
    }

    @PrivateApi
    public void destroyExecutor(String name) {
        final ExecutorService ex = executors.remove(name);
        if (ex != null) {
            ex.shutdown();
        }
    }

    private class ScheduledRunner implements Runnable {
        private final Runnable runnable;

        private ScheduledRunner(Runnable runnable) {
            this.runnable = runnable;
        }

        public void run() {
            execute("hz:scheduled", runnable);
        }
    }

//    private class ManagedExecutorService implements ExecutorService {
//
//        private final String name;
//
//        private final BlockingQueue<Object> controlQ;
//
//        private final AtomicInteger counter = new AtomicInteger(0);
//
//        private final int poolSize;
//
//        private ManagedExecutorService(String name, int maxThreadSize) {
//            this.name = name;
//            this.controlQ = new ArrayBlockingQueue<Object>(maxThreadSize);
//            for (int i = 0; i < maxThreadSize; i++) {
//                controlQ.offer(new Object());
//            }
//            this.poolSize = maxThreadSize;
//        }
//
//        public void execute(Runnable command) {
//            cachedExecutorService.execute(new ManagedRunnable(command, acquireLease()));
//        }
//
//        public <T> Future<T> submit(Callable<T> task) {
//            return cachedExecutorService.submit(new ManagedCallable<T>(task, acquireLease()));
//        }
//
//        public <T> Future<T> submit(Runnable task, T result) {
//            return cachedExecutorService.submit(new ManagedRunnable(task, acquireLease()), result);
//        }
//
//        public Future<?> submit(Runnable task) {
//            return cachedExecutorService.submit(new ManagedRunnable(task, acquireLease()));
//        }
//
//        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
//            throw new UnsupportedOperationException();
//        }
//
//        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
//            throw new UnsupportedOperationException();
//        }
//
//        public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
//            throw new UnsupportedOperationException();
//        }
//
//        public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
//            throw new UnsupportedOperationException();
//        }
//
//        private Lease acquireLease() {
//            final Object key;
//            try {
//                final int current = counter.incrementAndGet();
//                final int waitMillis = Math.max(1, current - poolSize + 1) * 500;
//                key = controlQ.poll(waitMillis, TimeUnit.MILLISECONDS);
//                if (key != null) {
//                    return new KeyLease(controlQ, counter, key);
//                } else {
//                    logger.log(Level.WARNING, "Executor[" + name + "] is overloaded! Pool-size: " + poolSize
//                            + ", Current-size: " + current);
//                    return new Lease(counter);
//                }
//            } catch (InterruptedException e) {
//                throw new RuntimeInterruptedException();
//            }
//        }
//
//        public void shutdown() {
//            controlQ.clear();
//        }
//
//        public List<Runnable> shutdownNow() {
//            shutdown();
//            return null;
//        }
//
//        public boolean isShutdown() {
//            return cachedExecutorService.isShutdown();
//        }
//
//        public boolean isTerminated() {
//            return cachedExecutorService.isTerminated();
//        }
//
//        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
//            return cachedExecutorService.awaitTermination(timeout, unit);
//        }
//    }
//
//    private class Lease {
//        final AtomicInteger counter;
//
//        private Lease(AtomicInteger counter) {
//            this.counter = counter;
//        }
//
//        void release() {
//            counter.decrementAndGet();
//        }
//    }
//
//    private class KeyLease extends Lease {
//        final BlockingQueue<Object> controlQ;
//        final Object key;
//
//        private KeyLease(BlockingQueue<Object> controlQ, AtomicInteger counter, Object key) {
//            super(counter);
//            this.controlQ = controlQ;
//            this.key = key;
//        }
//
//        public void release() {
//            controlQ.offer(key);
//            super.release();
//        }
//    }
//
//    private class ManagedRunnable implements Runnable {
//
//        private final Runnable runnable;
//
//        private final Lease lease;
//
//        private ManagedRunnable(Runnable runnable, Lease lease) {
//            this.runnable = runnable;
//            this.lease = lease;
//        }
//
//        public void run() {
//            try {
//                runnable.run();
//            } finally {
//                lease.release();
//            }
//        }
//    }
//
//    private class ManagedCallable<V> implements Callable<V> {
//
//        private final Callable<V> callable;
//
//        private final Lease lease;
//
//        private ManagedCallable(Callable<V> callable, Lease lease) {
//            this.callable = callable;
//            this.lease = lease;
//        }
//
//        public V call() throws Exception {
//            try {
//                return callable.call();
//            } finally {
//                lease.release();
//            }
//        }
//    }

}
