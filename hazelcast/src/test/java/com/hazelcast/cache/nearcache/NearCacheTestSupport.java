package com.hazelcast.cache.nearcache;

import com.hazelcast.cache.impl.nearcache.NearCacheContext;
import com.hazelcast.cache.impl.nearcache.NearCacheRecordStore;
import com.hazelcast.cache.impl.nearcache.impl.store.NearCacheDataRecordStore;
import com.hazelcast.cache.impl.nearcache.impl.store.NearCacheObjectRecordStore;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.nio.serialization.DefaultSerializationServiceBuilder;
import com.hazelcast.test.HazelcastTestSupport;

abstract class NearCacheTestSupport extends HazelcastTestSupport {

    protected NearCacheConfig createNearCacheConfig(String name, InMemoryFormat inMemoryFormat) {
        return
            new NearCacheConfig()
                    .setName(name)
                    .setInMemoryFormat(inMemoryFormat);
    }

    protected NearCacheContext createNearCacheContext() {
        return new NearCacheContext(new DefaultSerializationServiceBuilder().build(), null);
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

}
