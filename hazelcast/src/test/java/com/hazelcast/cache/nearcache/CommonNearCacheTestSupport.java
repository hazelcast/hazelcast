package com.hazelcast.cache.nearcache;

import com.hazelcast.cache.impl.nearcache.NearCacheContext;
import com.hazelcast.cache.impl.nearcache.NearCacheExecutor;
import com.hazelcast.cache.impl.nearcache.NearCacheRecordStore;
import com.hazelcast.cache.impl.nearcache.impl.store.NearCacheDataRecordStore;
import com.hazelcast.cache.impl.nearcache.impl.store.NearCacheObjectRecordStore;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public abstract class CommonNearCacheTestSupport extends HazelcastTestSupport {

    protected static final int DEFAULT_RECORD_COUNT = 100;
    protected static final String DEFAULT_NEAR_CACHE_NAME = "TestNearCache";

    protected List<ScheduledExecutorService> scheduledExecutorServices = new ArrayList<ScheduledExecutorService>();

    protected NearCacheConfig createNearCacheConfig(String name, InMemoryFormat inMemoryFormat) {
        return
                new NearCacheConfig()
                        .setName(name)
                        .setInMemoryFormat(inMemoryFormat);
    }

    protected NearCacheContext createNearCacheContext() {
        final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorServices.add(scheduledExecutorService);
        return new NearCacheContext(
                null, // No need to near-cache manager
                new DefaultSerializationServiceBuilder().build(),
                createNearCacheExecutor(),
                null);
    }

    protected NearCacheExecutor createNearCacheExecutor() {
        final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorServices.add(scheduledExecutorService);
        return new NearCacheExecutor() {
            @Override
            public ScheduledFuture<?> scheduleWithRepetition(Runnable command, long initialDelay,
                                                             long delay, TimeUnit unit) {
                return scheduledExecutorService.scheduleWithFixedDelay(command, initialDelay, delay, unit);
            }
        };
    }

    protected <K, V> NearCacheRecordStore<K, V> createNearCacheRecordStore(NearCacheConfig nearCacheConfig,
                                                                           NearCacheContext nearCacheContext, InMemoryFormat inMemoryFormat) {
        switch (inMemoryFormat) {
            case BINARY:
                return new NearCacheDataRecordStore<K, V>(nearCacheConfig, nearCacheContext);
            case OBJECT:
                return new NearCacheObjectRecordStore<K, V>(nearCacheConfig, nearCacheContext);
            default:
                throw new IllegalArgumentException("Unsupported in-memory format: " + inMemoryFormat);
        }
    }

    @After
    public void tearDown() {
        for (ScheduledExecutorService scheduledExecutorService : scheduledExecutorServices) {
            scheduledExecutorService.shutdown();
        }
        scheduledExecutorServices.clear();
    }

}
