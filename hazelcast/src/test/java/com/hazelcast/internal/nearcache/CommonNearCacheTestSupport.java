/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.nearcache;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.internal.nearcache.impl.store.NearCacheDataRecordStore;
import com.hazelcast.internal.nearcache.impl.store.NearCacheObjectRecordStore;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.TaskScheduler;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.util.executor.ExecutorType;
import com.hazelcast.util.executor.ManagedExecutorService;
import org.junit.After;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public abstract class CommonNearCacheTestSupport extends HazelcastTestSupport {

    protected static final int DEFAULT_RECORD_COUNT = 100;
    protected static final String DEFAULT_NEAR_CACHE_NAME = "TestNearCache";

    protected List<ScheduledExecutorService> scheduledExecutorServices = new ArrayList<ScheduledExecutorService>();
    protected SerializationService ss = new DefaultSerializationServiceBuilder()
            .setVersion(InternalSerializationService.VERSION_1).build();

    @After
    public final void shutdownExecutorServices() {
        for (ScheduledExecutorService scheduledExecutorService : scheduledExecutorServices) {
            scheduledExecutorService.shutdown();
        }
        scheduledExecutorServices.clear();
    }

    protected NearCacheConfig createNearCacheConfig(String name, InMemoryFormat inMemoryFormat) {
        return new NearCacheConfig()
                .setName(name)
                .setInMemoryFormat(inMemoryFormat);
    }

    protected <K, V> NearCacheRecordStore<K, V> createNearCacheRecordStore(NearCacheConfig nearCacheConfig,
                                                                           InMemoryFormat inMemoryFormat) {
        NearCacheRecordStore<K, V> recordStore = null;
        switch (inMemoryFormat) {
            case BINARY:
                recordStore = new NearCacheDataRecordStore<K, V>(DEFAULT_NEAR_CACHE_NAME, nearCacheConfig, ss, null);
                break;
            case OBJECT:
                recordStore = new NearCacheObjectRecordStore<K, V>(DEFAULT_NEAR_CACHE_NAME, nearCacheConfig, ss, null);
                break;
            default:
                throw new IllegalArgumentException("Unsupported in-memory format: " + inMemoryFormat);
        }
        recordStore.initialize();

        return recordStore;
    }

    protected ExecutionService createExecutionService() {
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorServices.add(scheduledExecutorService);
        return new TestExecutionService(scheduledExecutorService);
    }

    private static class TestExecutionService implements ExecutionService {

        private final ScheduledExecutorService executorService;

        private TestExecutionService(ScheduledExecutorService executorService) {
            this.executorService = executorService;
        }

        @Override
        public ManagedExecutorService register(String name, int poolSize, int queueCapacity, ExecutorType type) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ManagedExecutorService getExecutor(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void shutdownExecutor(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void execute(String name, Runnable command) {
            executorService.execute(command);
        }

        @Override
        public Future<?> submit(String name, Runnable task) {
            return executorService.submit(task);
        }

        @Override
        public <T> Future<T> submit(String name, Callable<T> task) {
            return executorService.submit(task);
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            return executorService.schedule(command, delay, unit);
        }

        @Override
        public ScheduledFuture<?> schedule(String name, Runnable command, long delay, TimeUnit unit) {
            return executorService.schedule(command, delay, unit);
        }

        @Override
        public ScheduledFuture<?> scheduleWithRepetition(Runnable command, long initialDelay, long period, TimeUnit unit) {
            return executorService.scheduleAtFixedRate(command, initialDelay, period, unit);
        }

        @Override
        public ScheduledFuture<?> scheduleWithRepetition(String name, Runnable command, long initialDelay, long period, TimeUnit unit) {
            return executorService.scheduleAtFixedRate(command, initialDelay, period, unit);
        }

        @Override
        public TaskScheduler getGlobalTaskScheduler() {
            throw new UnsupportedOperationException();
        }

        @Override
        public TaskScheduler getTaskScheduler(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <V> ICompletableFuture<V> asCompletableFuture(Future<V> future) {
            throw new UnsupportedOperationException();
        }
    }
}
