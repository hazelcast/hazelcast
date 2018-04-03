/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.merge;

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.cache.CacheMergePolicy;
import com.hazelcast.cache.CacheTestSupport;
import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.operation.CacheMergeOperation;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import java.io.Serializable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;


@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CacheMergeOperationUpdateEventTest extends CacheTestSupport {

    protected ICache<Integer, Integer> cache;
    protected TestMergePolicy mergePolicy = new TestMergePolicy();

    private HazelcastInstance instance;
    private InternalSerializationService ss;

    private TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
    private TestCacheEntryUpdatedListener updateListener = new TestCacheEntryUpdatedListener();

    @Override
    protected void onSetup() {
        Config config = getConfig();
        instance = factory.newHazelcastInstance(config);
        ss = getSerializationService(instance);
    }

    @Override
    protected void onTearDown() {
        factory.shutdownAll();
    }

    @Override
    protected <K, V> CacheConfig<K, V> createCacheConfig() {
        CacheConfig cacheConfig = super.createCacheConfig();
        cacheConfig.setInMemoryFormat(getInMemoryFormat());

        MutableCacheEntryListenerConfiguration listenerConfiguration =
                new MutableCacheEntryListenerConfiguration(
                        FactoryBuilder.factoryOf(updateListener), null, true, true);

        cacheConfig.addCacheEntryListenerConfiguration(listenerConfiguration);

        return cacheConfig;
    }

    protected InMemoryFormat getInMemoryFormat() {
        return InMemoryFormat.BINARY;
    }

    @Override
    protected HazelcastInstance getHazelcastInstance() {
        return instance;
    }

    @Test
    public void merge_operation_should_not_fire_update_event_when_values_equal()
            throws ExecutionException, InterruptedException {
        cache = createCache();

        int key = 1;
        int value = 1;

        cache.put(key, value);

        Data dataKey = ss.toData(key);
        Data dataExistingValue = ss.toData(value);

        Operation operation = createMergeOperation(dataKey, dataExistingValue);
        executeOperation(operation, getPartitionService(instance).getPartitionId(dataKey));

        assertNoUpdateEventFired();
    }

    private void assertNoUpdateEventFired() {
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertEquals(0, updateListener.updated.get());
            }
        }, 5);
    }

    private void executeOperation(Operation operation, int partitionId) throws InterruptedException, ExecutionException {
        Future future = getOperationServiceImpl(instance).invokeOnPartition(CacheService.SERVICE_NAME, operation, partitionId);
        future.get();
    }

    private Operation createMergeOperation(Data dataKey, Data dataExistingValue) {
        TestCacheEntryView cacheEntryView = new TestCacheEntryView(dataKey, dataExistingValue);
        Operation operation = newMergeOperation(dataKey, cacheEntryView);
        operation.setServiceName(CacheService.SERVICE_NAME);
        return operation;
    }

    protected Operation newMergeOperation(Data dataKey, CacheEntryView<Data, Data> cacheEntryView) {
        return new CacheMergeOperation("/hz/" + cache.getName(), dataKey, cacheEntryView, mergePolicy);
    }

    private class TestCacheEntryView implements CacheEntryView<Data, Data> {

        private Data dataKey;
        private Data dataValue;

        public TestCacheEntryView(Data dataKey, Data dataValue) {
            this.dataKey = dataKey;
            this.dataValue = dataValue;
        }

        @Override
        public Data getKey() {
            return dataKey;
        }

        @Override
        public Data getValue() {
            return dataValue;
        }

        @Override
        public long getExpirationTime() {
            return CacheRecord.TIME_NOT_AVAILABLE;
        }

        @Override
        public long getCreationTime() {
            return CacheRecord.TIME_NOT_AVAILABLE;
        }

        @Override
        public long getLastAccessTime() {
            return CacheRecord.TIME_NOT_AVAILABLE;
        }

        @Override
        public long getAccessHit() {
            return CacheRecord.TIME_NOT_AVAILABLE;
        }
    }

    private class TestMergePolicy implements CacheMergePolicy {

        @Override
        public Object merge(String cacheName, CacheEntryView mergingEntry, CacheEntryView existingEntry) {
            return mergingEntry.getValue();
        }
    }

    private class TestCacheEntryUpdatedListener<K, V> implements CacheEntryUpdatedListener<K, V>, Serializable {

        public AtomicInteger updated = new AtomicInteger();

        public TestCacheEntryUpdatedListener() {
        }

        @Override
        public void onUpdated(Iterable<CacheEntryEvent<? extends K, ? extends V>> cacheEntryEvents)
                throws CacheEntryListenerException {
            for (CacheEntryEvent<? extends K, ? extends V> event : cacheEntryEvents) {
                updated.incrementAndGet();
            }
        }
    }
}
