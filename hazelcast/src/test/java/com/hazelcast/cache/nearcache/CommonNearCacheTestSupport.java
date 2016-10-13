package com.hazelcast.cache.nearcache;

import com.hazelcast.cache.impl.nearcache.NearCacheContext;
import com.hazelcast.cache.impl.nearcache.NearCacheRecordStore;
import com.hazelcast.cache.impl.nearcache.impl.store.NearCacheDataRecordStore;
import com.hazelcast.cache.impl.nearcache.impl.store.NearCacheObjectRecordStore;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.TaskScheduler;
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

    protected NearCacheContext createNearCacheContext() {
        return new NearCacheContext(new DefaultSerializationServiceBuilder().build(), createExecutionService());
    }

    protected ExecutionService createExecutionService() {
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorServices.add(scheduledExecutorService);
        return new TestExecutionService(scheduledExecutorService);
    }

    protected <K, V> NearCacheRecordStore<K, V> createNearCacheRecordStore(NearCacheConfig nearCacheConfig,
                                                                           NearCacheContext nearCacheContext,
                                                                           InMemoryFormat inMemoryFormat) {
        switch (inMemoryFormat) {
            case BINARY:
                return new NearCacheDataRecordStore<K, V>(nearCacheConfig, nearCacheContext);
            case OBJECT:
                return new NearCacheObjectRecordStore<K, V>(nearCacheConfig, nearCacheContext);
            default:
                throw new IllegalArgumentException("Unsupported in-memory format: " + inMemoryFormat);
        }
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
