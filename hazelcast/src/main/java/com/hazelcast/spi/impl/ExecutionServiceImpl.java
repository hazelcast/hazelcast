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

import com.hazelcast.executor.ExecutorThreadFactory;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.annotation.PrivateApi;

import java.util.concurrent.*;
import java.util.logging.Level;

/**
 * @mdogan 12/14/12
 */
final class ExecutionServiceImpl implements ExecutionService {

    final ExecutorService cachedExecutorService;
    final ExecutorService eventExecutorService;
    final ScheduledExecutorService scheduledExecutorService;
    private final ILogger logger;

    public ExecutionServiceImpl(NodeEngineImpl nodeEngine) {
        Node node = nodeEngine.getNode();
        logger = node.getLogger(ExecutionService.class.getName());
        final ClassLoader classLoader = node.getConfig().getClassLoader();
        final ExecutorThreadFactory threadFactory = new ExecutorThreadFactory(node.threadGroup, node.hazelcastInstance,
                node.getThreadPoolNamePrefix("cached"), classLoader);
        cachedExecutorService = new ThreadPoolExecutor(
                3, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(), threadFactory);
//        cachedExecutorService = Executors.newFixedThreadPool(40, threadFactory);
        eventExecutorService = Executors.newSingleThreadExecutor(
                new ExecutorThreadFactory(node.threadGroup, node.hazelcastInstance,
                        node.getThreadPoolNamePrefix("event"), node.getConfig().getClassLoader()));
        scheduledExecutorService = Executors.newScheduledThreadPool(2,
                new ExecutorThreadFactory(node.threadGroup,
                        node.hazelcastInstance,
                        node.getThreadPoolNamePrefix("scheduled"), classLoader));
    }

    public Future<?> submit(Runnable task) {
        return cachedExecutorService.submit(task);
    }

    public void execute(final Runnable command) {
        cachedExecutorService.execute(command);
    }

    public void schedule(final Runnable command, long delay, TimeUnit unit) {
        scheduledExecutorService.schedule(command, delay, unit);
    }

    public void scheduleAtFixedRate(final Runnable command, long initialDelay, long period, TimeUnit unit) {
        scheduledExecutorService.scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    public void scheduleWithFixedDelay(final Runnable command, long initialDelay, long period, TimeUnit unit) {
        scheduledExecutorService.scheduleWithFixedDelay(command, initialDelay, period, unit);
    }

    @PrivateApi
    void shutdown() {
        cachedExecutorService.shutdown();
        scheduledExecutorService.shutdownNow();
        eventExecutorService.shutdownNow();
        try {
            cachedExecutorService.awaitTermination(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.log(Level.FINEST, e.getMessage(), e);
        }
    }
}
