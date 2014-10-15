package com.hazelcast.cache;

import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.cache.impl.record.CacheRecordFactory;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.serialization.SerializationServiceBuilder;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Serialization test class for JCache
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class CacheSerializationTest extends HazelcastTestSupport {

    SerializationService service;

    @Before
    public void setup() {
        SerializationServiceBuilder builder = new DefaultSerializationServiceBuilder();
        service = builder.build();
    }

    @After
    public void tearDown() {
        service.destroy();
    }

    @Test
    public void testCacheRecord_withBinaryInMemoryData() {
        CacheRecord cacheRecord = createRecord(InMemoryFormat.BINARY);

        Data cacheRecordData = service.toData(cacheRecord);
        service.toObject(cacheRecordData);
    }

    @Test
    public void testCacheRecord_withObjectInMemoryData() {
        CacheRecord cacheRecord = createRecord(InMemoryFormat.OBJECT);

        Data cacheRecordData = service.toData(cacheRecord);
        service.toObject(cacheRecordData);
    }

    private CacheRecord createRecord(InMemoryFormat format) {
        CacheRecordFactory factory = new CacheRecordFactory(format, service);
        Data key = service.toData(randomString());
        return factory.newRecord(key, randomString());
    }



}
