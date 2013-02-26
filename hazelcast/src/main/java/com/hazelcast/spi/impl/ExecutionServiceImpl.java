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
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.executor.ExecutorThreadFactory;
import com.hazelcast.util.executor.ManagedExecutorService;
import com.hazelcast.util.executor.PoolExecutorThreadFactory;

import java.util.Collection;
import java.util.List;
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
        final Node node = nodeEngine.getNode();
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

        final String scheduledThreadName = node.getThreadNamePrefix("scheduled");
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
                new ExecutorThreadFactory(node.threadGroup, classLoader) {
                    protected String newThreadName() {
                        return scheduledThreadName;
                    }
                });

        // default executors
        register("hz:system", 30, Integer.MAX_VALUE);
        register("hz:scheduled", 20, Integer.MAX_VALUE);
    }

    ExecutorService register(String name, int maxThreadSize, int queueSize) {
        final ManagedExecutorService executor = new ManagedExecutorService(name, cachedExecutorService,
                maxThreadSize, queueSize);
        if (executors.putIfAbsent(name, executor) != null) {
            throw new IllegalArgumentException();
        }
        return executor;
    }

    private final ConcurrencyUtil.ConstructorFunction<String, ExecutorService> constructor =
            new ConcurrencyUtil.ConstructorFunction<String, ExecutorService>() {
                public ManagedExecutorService createNew(String name) {
                    final ExecutorConfig cfg = nodeEngine.getConfig().getExecutorConfig(name);
                    final int queueCapacity = cfg.getQueueCapacity() == 0 ? Integer.MAX_VALUE : cfg.getQueueCapacity();
                    return new ManagedExecutorService(name, cachedExecutorService, cfg.getPoolSize(), queueCapacity);
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

    public ScheduledExecutorService getScheduledExecutor() {
        return new ScheduledExecutorServiceDelegate(scheduledExecutorService);
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
            try {
                execute("hz:scheduled", runnable);
            } catch (Throwable t) {
                logger.log(Level.SEVERE, t.getMessage(), t);
            }
        }
    }

    private static class ScheduledExecutorServiceDelegate implements ScheduledExecutorService {

        private final ScheduledExecutorService scheduledExecutorService;

        private ScheduledExecutorServiceDelegate(ScheduledExecutorService scheduledExecutorService) {
            this.scheduledExecutorService = scheduledExecutorService;
        }

        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            return scheduledExecutorService.schedule(command, delay, unit);
        }

        public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
            return scheduledExecutorService.schedule(callable, delay, unit);
        }

        public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
            return scheduledExecutorService.scheduleAtFixedRate(command, initialDelay, period, unit);
        }

        public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
            return scheduledExecutorService.scheduleWithFixedDelay(command, initialDelay, delay, unit);
        }

        public void shutdown() {
            throw new UnsupportedOperationException();
        }

        public List<Runnable> shutdownNow() {
            throw new UnsupportedOperationException();
        }

        public boolean isShutdown() {
            return false;
        }

        public boolean isTerminated() {
            return false;
        }

        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            throw new UnsupportedOperationException();
        }

        public <T> Future<T> submit(Callable<T> task) {
            throw new UnsupportedOperationException();
        }

        public <T> Future<T> submit(Runnable task, T result) {
            throw new UnsupportedOperationException();
        }

        public Future<?> submit(Runnable task) {
            throw new UnsupportedOperationException();
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

        public void execute(Runnable command) {
            throw new UnsupportedOperationException();
        }
    }


}
