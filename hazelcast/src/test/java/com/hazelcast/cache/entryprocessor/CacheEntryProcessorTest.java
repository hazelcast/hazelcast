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

package com.hazelcast.cache.entryprocessor;

import com.hazelcast.cache.BackupAwareEntryProcessor;
import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import javax.cache.spi.CachingProvider;
import java.io.Serializable;
import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class CacheEntryProcessorTest extends HazelcastTestSupport {

    private static final int ASSERTION_TIMEOUT_SECONDS = 300;

    private static TestHazelcastInstanceFactory factory;
    private static HazelcastInstance node1;
    private static HazelcastInstance node2;

    private static CacheService cacheServiceOnNode1;
    private static CacheService cacheServiceOnNode2;

    private static SerializationService serializationService;

    @BeforeClass
    public static void setUp() throws Exception {
        setUpInternal();
    }

    @AfterClass
    public static void tearDown() {
        factory.shutdownAll();
    }

    /**
     * If there is not any implemented backup entry processor,
     * we are only sending the result of execution to the backup node.
     */
    @Test
    public void whenBackupEntryProcessor_isNotImplemented() throws Exception {
        EntryProcessor<Integer, String, Void> entryProcessor = new SimpleEntryProcessor();
        executeTestInternal(entryProcessor);
    }

    @Test
    public void whenBackupEntryProcessor_isImplemented() throws Exception {
        EntryProcessor<Integer, String, Void> entryProcessor = new CustomBackupAwareEntryProcessor();
        executeTestInternal(entryProcessor);
    }

    @Test
    public void whenBackupEntryProcessor_isSame_withPrimaryEntryProcessor() throws Exception {
        EntryProcessor<Integer, String, Void> entryProcessor = new SimpleBackupAwareEntryProcessor();
        executeTestInternal(entryProcessor);
    }

    @Test
    public void whenBackupEntryProcessor_isNull() throws Exception {
        EntryProcessor<Integer, String, Void> entryProcessor = new NullBackupAwareEntryProcessor();
        executeTestInternal(entryProcessor);
    }

    @Test
    public void removeRecordWithEntryProcessor() {
        final int ENTRY_COUNT = 10;

        CachingProvider cachingProvider = HazelcastServerCachingProvider.createCachingProvider(node1);
        CacheManager cacheManager = cachingProvider.getCacheManager();
        CompleteConfiguration<Integer, String> cacheConfig =
                new MutableConfiguration<Integer, String>()
                        .setTypes(Integer.class, String.class);
        ICache<Integer, String> cache = cacheManager.createCache("MyCache", cacheConfig).unwrap(ICache.class);

        for (int i = 0; i < ENTRY_COUNT; i++) {
            cache.put(i * 1000, "Value-" + (i * 1000));
        }

        assertEquals(ENTRY_COUNT, cache.size());

        for (int i = 0; i < ENTRY_COUNT; i++) {
            if (i % 2 == 0) {
                cache.invoke(i * 1000, new RemoveRecordEntryProcessor());
            }
        }

        assertEquals(ENTRY_COUNT / 2, cache.size());
    }

    @Test
    public void givenEntryExists_whenCallsGetValue_thenSubsequentExistsStillReturnTrue() {
        String cacheName = randomName();
        int key = 1;

        CachingProvider cachingProvider = HazelcastServerCachingProvider.createCachingProvider(node1);
        CacheManager cacheManager = cachingProvider.getCacheManager();
        CompleteConfiguration<Integer, String> config =
                new MutableConfiguration<Integer, String>()
                        .setTypes(Integer.class, String.class);
        Cache<Integer, String> cache = cacheManager.createCache(cacheName, config);

        cache.put(key, "foo");

        boolean result = cache.invoke(key, new GetAndCheckEntry());
        assertTrue(result);
    }

    private void executeTestInternal(EntryProcessor<Integer, String, Void> entryProcessor) {
        final String cacheName = randomString();
        final Integer key = 1;

        executeEntryProcessor(key, entryProcessor, cacheName);

        assertKeyExistsInCache("Foo", key, cacheName, cacheServiceOnNode1);
        assertKeyExistsInCache("Foo", key, cacheName, cacheServiceOnNode2);
    }

    private void assertKeyExistsInCache(final String expectedValue, final Integer key,
                                        final String cacheName, final CacheService cacheService) {
        assertTrueEventually(new AssertTask() {

            @Override
            public void run() throws Exception {

                final Data dataKey = serializationService.toData(key);
                final PartitionService partitionService = node1.getPartitionService();
                final Partition partition = partitionService.getPartition(key);
                final int partitionId = partition.getPartitionId();
                final ICacheRecordStore recordStore = getRecordStore(cacheService, cacheName, partitionId);
                final CacheRecord record = recordStore.getRecord(dataKey);

                assertNotNull("Backups are not done yet!!!", record);

                final Object value = serializationService.toObject(record.getValue());
                assertEquals(expectedValue, value);
            }
        }, ASSERTION_TIMEOUT_SECONDS);

    }

    public static class SimpleEntryProcessor
            implements EntryProcessor<Integer, String, Void>, Serializable {

        private static final long serialVersionUID = -396575576353368113L;

        @Override
        public Void process(MutableEntry<Integer, String> entry, Object... arguments)
                throws EntryProcessorException {

            entry.setValue("Foo");
            return null;
        }
    }

    public static class SimpleBackupAwareEntryProcessor
            implements BackupAwareEntryProcessor<Integer, String, Void>, Serializable {

        private static final long serialVersionUID = -5274605583423489718L;

        @Override
        public Void process(MutableEntry<Integer, String> entry, Object... arguments)
                throws EntryProcessorException {

            entry.setValue("Foo");
            return null;
        }

        @Override
        public EntryProcessor<Integer, String, Void> createBackupEntryProcessor() {
            return this;
        }
    }

    public static class NullBackupAwareEntryProcessor
            implements BackupAwareEntryProcessor<Integer, String, Void>, Serializable {

        private static final long serialVersionUID = -8423196656316041614L;

        @Override
        public Void process(MutableEntry<Integer, String> entry, Object... arguments)
                throws EntryProcessorException {

            entry.setValue("Foo");
            return null;
        }

        @Override
        public EntryProcessor<Integer, String, Void> createBackupEntryProcessor() {
            return null;
        }
    }

    public static class CustomBackupAwareEntryProcessor
            implements BackupAwareEntryProcessor<Integer, String, Void>, Serializable {

        private static final long serialVersionUID = 3409663318028125754L;

        @Override
        public Void process(MutableEntry<Integer, String> entry, Object... arguments)
                throws EntryProcessorException {

            entry.setValue("Foo");
            return null;
        }

        @Override
        public EntryProcessor<Integer, String, Void> createBackupEntryProcessor() {
            return new BackupEntryProcessor();
        }
    }

    public static class BackupEntryProcessor
            implements EntryProcessor<Integer, String, Void>, Serializable {

        private static final long serialVersionUID = -6376894786246368848L;

        @Override
        public Void process(MutableEntry<Integer, String> entry, Object... arguments)
                throws EntryProcessorException {

            entry.setValue("Foo");
            return null;
        }
    }

    public static class RemoveRecordEntryProcessor
            implements EntryProcessor<Integer, String, Void>, Serializable {

        @Override
        public Void process(MutableEntry<Integer, String> entry, Object... arguments)
                throws EntryProcessorException {
            entry.remove();
            return null;
        }
    }

    private void executeEntryProcessor(Integer key, EntryProcessor<Integer, String, Void> entryProcessor, String cacheName) {
        CachingProvider cachingProvider = HazelcastServerCachingProvider.createCachingProvider(node1);
        CacheManager cacheManager = cachingProvider.getCacheManager();

        CompleteConfiguration<Integer, String> config =
                new MutableConfiguration<Integer, String>()
                        .setTypes(Integer.class, String.class);

        Cache<Integer, String> cache = cacheManager.createCache(cacheName, config);

        cache.invoke(key, entryProcessor);
    }

    private ICacheRecordStore getRecordStore(CacheService cacheService, String cacheName, int partitionId) {
        try {
            return cacheService.getOrCreateRecordStore("/hz/" + cacheName, partitionId);
        } catch (Exception e) {
            fail("CacheRecordStore not yet initialized!!!");
        }
        return null;
    }

    private static void setUpInternal() throws NoSuchFieldException, IllegalAccessException {
        factory = new TestHazelcastInstanceFactory(2);
        node1 = factory.newHazelcastInstance();
        node2 = factory.newHazelcastInstance();

        Field original = HazelcastInstanceProxy.class.getDeclaredField("original");
        original.setAccessible(true);

        HazelcastInstanceImpl impl1 = (HazelcastInstanceImpl) original.get(node1);
        HazelcastInstanceImpl impl2 = (HazelcastInstanceImpl) original.get(node2);

        cacheServiceOnNode1 = impl1.node.getNodeEngine().getService(CacheService.SERVICE_NAME);
        cacheServiceOnNode2 = impl2.node.getNodeEngine().getService(CacheService.SERVICE_NAME);

        serializationService = impl1.node.getNodeEngine().getSerializationService();
    }

    private static class GetAndCheckEntry implements EntryProcessor<Integer, String, Boolean>, Serializable {
        @Override
        public Boolean process(MutableEntry<Integer, String> entry, Object... arguments) throws EntryProcessorException {
            entry.getValue();
            return entry.exists();
        }
    }
}
