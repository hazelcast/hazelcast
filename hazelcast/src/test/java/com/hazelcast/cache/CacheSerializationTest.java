package com.hazelcast.cache;

import com.hazelcast.cache.impl.CachePartitionSegment;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.cache.impl.operation.CacheReplicationOperation;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.cache.impl.record.CacheRecordFactory;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.serialization.SerializationServiceBuilder;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.spi.CachingProvider;
import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;

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

    @Test
    public void test_CacheReplicationOperation_serialization() throws Exception {
        TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(1);
        HazelcastInstance hazelcastInstance = factory.newHazelcastInstance();

        try {
            CachingProvider provider = HazelcastServerCachingProvider.createCachingProvider(hazelcastInstance);
            CacheManager manager = provider.getCacheManager();

            CompleteConfiguration configuration = new MutableConfiguration();
            Cache cache1 = manager.createCache("cache1", configuration);
            Cache cache2 = manager.createCache("cache2", configuration);
            Cache cache3 = manager.createCache("cache3", configuration);

            for (int i = 0; i < 1000; i++) {
                cache1.put("key" + i, i);
                cache2.put("key" + i, i);
                cache3.put("key" + i, i);
            }

            HazelcastInstanceProxy proxy = (HazelcastInstanceProxy) hazelcastInstance;

            Field original = HazelcastInstanceProxy.class.getDeclaredField("original");
            original.setAccessible(true);

            HazelcastInstanceImpl impl = (HazelcastInstanceImpl) original.get(proxy);
            NodeEngineImpl nodeEngine = impl.node.nodeEngine;
            CacheService cacheService = nodeEngine.getService(CacheService.SERVICE_NAME);

            int partitionCount = nodeEngine.getPartitionService().getPartitionCount();

            for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
                CachePartitionSegment segment = cacheService.getSegment(partitionId);

                CacheReplicationOperation operation = new CacheReplicationOperation(segment, 1);
                Data serialized = service.toData(operation);
                try {
                    service.toObject(serialized);
                } catch (Exception e) {
                    throw new Exception("Partition: " + partitionId, e);
                }
            }

        } finally {
            factory.shutdownAll();
        }
    }

    private CacheRecord createRecord(InMemoryFormat format) {
        CacheRecordFactory factory = new CacheRecordFactory(format, service);
        return factory.newRecord(randomString());
    }

}
