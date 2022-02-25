/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache;

import com.hazelcast.cache.impl.CachePartitionEventData;
import com.hazelcast.cache.impl.CachePartitionSegment;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.operation.CacheReplicationOperation;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.cache.impl.record.CacheRecordFactory;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.SerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.CollectionUtil;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
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
import java.net.UnknownHostException;
import java.util.Collection;

import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static org.junit.Assert.assertEquals;

/**
 * Serialization test class for JCache
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CacheSerializationTest extends HazelcastTestSupport {

    SerializationService service;

    @Before
    public void setup() {
        SerializationServiceBuilder builder = new DefaultSerializationServiceBuilder();
        service = builder.build();
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testCacheRecord_withBinaryInMemoryData() {
        String value = randomString();
        CacheRecord cacheRecord = createRecord(InMemoryFormat.BINARY, value);

        Data cacheRecordData = service.toData(cacheRecord);
        CacheRecord deserialized = service.toObject(cacheRecordData);
        assertEquals(value, service.toObject(deserialized.getValue()));
    }

    @Test
    public void testCacheRecord_withObjectInMemoryData() {
        String value = randomString();
        CacheRecord cacheRecord = createRecord(InMemoryFormat.OBJECT, value);

        Data cacheRecordData = service.toData(cacheRecord);
        CacheRecord deserialized = service.toObject(cacheRecordData);
        assertEquals(value, deserialized.getValue());
    }

    @Test
    public void test_CacheReplicationOperation_serialization() throws Exception {
        TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(1);
        HazelcastInstance hazelcastInstance = factory.newHazelcastInstance();

        try {
            CachingProvider provider = createServerCachingProvider(hazelcastInstance);
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

                int replicaIndex = 1;
                Collection<ServiceNamespace> namespaces = segment.getAllNamespaces(replicaIndex);
                if (CollectionUtil.isEmpty(namespaces)) {
                    continue;
                }

                CacheReplicationOperation operation = new CacheReplicationOperation();
                operation.prepare(segment, namespaces, replicaIndex);
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

    @Test
    public void testCachePartitionEventData() throws UnknownHostException {
        Address address = new Address("127.0.0.1", 5701);
        Member member = new MemberImpl(address, MemberVersion.UNKNOWN, true);
        CachePartitionEventData cachePartitionEventData = new CachePartitionEventData("test", 1, member);
        CachePartitionEventData deserialized = service.toObject(cachePartitionEventData);
        assertEquals(cachePartitionEventData, deserialized);
    }

    private CacheRecord createRecord(InMemoryFormat format, String value) {
        CacheRecordFactory factory = new CacheRecordFactory(format, service);
        return factory.newRecordWithExpiry(value, Clock.currentTimeMillis(), -1);
    }

}
