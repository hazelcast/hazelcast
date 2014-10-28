/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Partition;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
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

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class JCacheEntryProcessorTest extends HazelcastTestSupport {

    private static TestHazelcastInstanceFactory factory;
    private static HazelcastInstance hz1;
    private static HazelcastInstance hz2;

    private static CacheService cacheServiceHz1;
    private static CacheService cacheServiceHz2;

    private static SerializationService serializationService;

    @BeforeClass
    public static void setup() throws Exception {
        factory = new TestHazelcastInstanceFactory(2);
        hz1 = factory.newHazelcastInstance();
        hz2 = factory.newHazelcastInstance();

        Field original = HazelcastInstanceProxy.class.getDeclaredField("original");
        original.setAccessible(true);

        HazelcastInstanceImpl impl1 = (HazelcastInstanceImpl) original.get(hz1);
        HazelcastInstanceImpl impl2 = (HazelcastInstanceImpl) original.get(hz2);

        cacheServiceHz1 = impl1.node.getNodeEngine().getService(CacheService.SERVICE_NAME);
        cacheServiceHz2 = impl2.node.getNodeEngine().getService(CacheService.SERVICE_NAME);

        serializationService = impl1.node.getNodeEngine().getSerializationService();
    }

    @AfterClass
    public static void teardown() {
        factory.shutdownAll();
    }

    @Test
    public void test_execution_entryprocessor_default_backup() throws Exception {
        CachingProvider cachingProvider = HazelcastServerCachingProvider.createCachingProvider(hz1);
        CacheManager cacheManager = cachingProvider.getCacheManager();

        String cacheName = randomString();

        CompleteConfiguration<Integer, String> config =
                new MutableConfiguration<Integer, String>()
                        .setTypes(Integer.class, String.class);

        Cache<Integer, String> cache = cacheManager.createCache(cacheName, config);

        cache.invoke(1, new SimpleEntryProcessor());

        final Data key = serializationService.toData(1);
        final int partitionId = hz1.getPartitionService().getPartition(1).getPartitionId();

        final ICacheRecordStore recordStore1 = cacheServiceHz1.getCache("/hz/" + cacheName, partitionId);
        final ICacheRecordStore recordStore2 = cacheServiceHz2.getCache("/hz/" + cacheName, partitionId);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                CacheRecord record1 = recordStore1.getRecord(key);
                CacheRecord record2 = recordStore2.getRecord(key);

                Object value1 = serializationService.toObject(record1.getValue());
                Object value2 = serializationService.toObject(record2.getValue());

                assertEquals("Foo", value1);
                assertEquals("Foo", value2);
            }
        });
    }

    @Test
    public void test_execution_entryprocessor_with_backup_entry_processor() throws Exception {
        CachingProvider cachingProvider = HazelcastServerCachingProvider.createCachingProvider(hz1);
        CacheManager cacheManager = cachingProvider.getCacheManager();

        String cacheName = randomString();

        CompleteConfiguration<Integer, String> config =
                new MutableConfiguration<Integer, String>()
                        .setTypes(Integer.class, String.class);

        Cache<Integer, String> cache = cacheManager.createCache(cacheName, config);

        cache.invoke(1, new SimpleBackupAwareEntryProcessor());

        final Data key = serializationService.toData(1);
        final int partitionId = hz1.getPartitionService().getPartition(1).getPartitionId();

        final ICacheRecordStore recordStore1 = cacheServiceHz1.getCache("/hz/" + cacheName, partitionId);
        final ICacheRecordStore recordStore2 = cacheServiceHz2.getCache("/hz/" + cacheName, partitionId);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                CacheRecord record1 = recordStore1.getRecord(key);
                CacheRecord record2 = recordStore2.getRecord(key);

                Object value1 = serializationService.toObject(record1.getValue());
                Object value2 = serializationService.toObject(record2.getValue());

                assertEquals("Foo", value1);
                assertEquals("Foo", value2);
            }
        });
    }

    @Test
    public void test_execution_entryprocessor_with_backup_entry_processor_null() throws Exception {
        CachingProvider cachingProvider = HazelcastServerCachingProvider.createCachingProvider(hz1);
        CacheManager cacheManager = cachingProvider.getCacheManager();

        String cacheName = randomString();

        CompleteConfiguration<Integer, String> config =
                new MutableConfiguration<Integer, String>()
                        .setTypes(Integer.class, String.class);

        Cache<Integer, String> cache = cacheManager.createCache(cacheName, config);

        cache.invoke(1, new NullBackupAwareEntryProcessor());

        final Data key = serializationService.toData(1);
        final int partitionId = hz1.getPartitionService().getPartition(1).getPartitionId();

        final ICacheRecordStore recordStore1 = cacheServiceHz1.getCache("/hz/" + cacheName, partitionId);
        final ICacheRecordStore recordStore2 = cacheServiceHz2.getCache("/hz/" + cacheName, partitionId);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                CacheRecord record1 = recordStore1.getRecord(key);
                CacheRecord record2 = recordStore2.getRecord(key);

                Object value1 = serializationService.toObject(record1.getValue());
                Object value2 = serializationService.toObject(record2.getValue());

                assertEquals("Foo", value1);
                assertEquals("Foo", value2);
            }
        });
    }

    @Test
    public void test_execution_entryprocessor_with_backup_entry_processor_custom_backup() throws Exception {
        CachingProvider cachingProvider = HazelcastServerCachingProvider.createCachingProvider(hz1);
        CacheManager cacheManager = cachingProvider.getCacheManager();

        String cacheName = randomString();

        CompleteConfiguration<Integer, String> config =
                new MutableConfiguration<Integer, String>()
                        .setTypes(Integer.class, String.class);

        Cache<Integer, String> cache = cacheManager.createCache(cacheName, config);

        cache.invoke(1, new CustomBackupAwareEntryProcessor());

        final Data key = serializationService.toData(1);
        final int partitionId = hz1.getPartitionService().getPartition(1).getPartitionId();

        final ICacheRecordStore recordStore1 = cacheServiceHz1.getCache("/hz/" + cacheName, partitionId);
        final ICacheRecordStore recordStore2 = cacheServiceHz2.getCache("/hz/" + cacheName, partitionId);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                CacheRecord record1 = recordStore1.getRecord(key);
                CacheRecord record2 = recordStore2.getRecord(key);

                Object value1 = serializationService.toObject(record1.getValue());
                Object value2 = serializationService.toObject(record2.getValue());

                Partition partition = hz1.getPartitionService().getPartition(1);
                if (hz1.getCluster().getLocalMember().equals(partition.getOwner())) {
                    assertEquals("Foo1", value1);
                    assertEquals("Foo2", value2);
                } else {
                    assertEquals("Foo1", value2);
                    assertEquals("Foo2", value1);
                }
            }
        });
    }

    public static class SimpleEntryProcessor
            implements EntryProcessor<Integer, String, Void>, Serializable {

        @Override
        public Void process(MutableEntry<Integer, String> entry, Object... arguments)
                throws EntryProcessorException {

            entry.setValue("Foo");
            return null;
        }
    }

    public static class SimpleBackupAwareEntryProcessor
            implements BackupAwareEntryProcessor<Integer, String, Void>, Serializable {

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

        @Override
        public Void process(MutableEntry<Integer, String> entry, Object... arguments)
                throws EntryProcessorException {

            entry.setValue("Foo1");
            return null;
        }

        @Override
        public EntryProcessor<Integer, String, Void> createBackupEntryProcessor() {
            return new BackupEntryProcessor();
        }
    }

    public static class BackupEntryProcessor
            implements EntryProcessor<Integer, String, Void>, Serializable {

        @Override
        public Void process(MutableEntry<Integer, String> entry, Object... arguments)
                throws EntryProcessorException {

            entry.setValue("Foo2");
            return null;
        }
    }
}
