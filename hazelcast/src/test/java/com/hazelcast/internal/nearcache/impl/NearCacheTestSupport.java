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

package com.hazelcast.internal.nearcache.impl;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.adapter.DataStructureAdapter;
import com.hazelcast.internal.monitor.impl.NearCacheStatsImpl;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.NearCacheRecord;
import com.hazelcast.internal.nearcache.NearCacheRecordStore;
import com.hazelcast.internal.nearcache.impl.invalidation.StaleReadDetector;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nearcache.NearCacheStats;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.AssertTask;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.config.NearCacheConfig.DEFAULT_MEMORY_FORMAT;
import static com.hazelcast.internal.nearcache.NearCache.DEFAULT_EXPIRATION_TASK_INITIAL_DELAY_SECONDS;
import static com.hazelcast.internal.nearcache.NearCache.UpdateSemantic.READ_UPDATE;
import static com.hazelcast.internal.nearcache.NearCacheRecord.NOT_RESERVED;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("WeakerAccess")
public abstract class NearCacheTestSupport extends CommonNearCacheTestSupport {

    protected SerializationService ss;
    protected HazelcastProperties properties;
    protected ExecutionService executionService;

    @Before
    public void setUp() throws Exception {
        HazelcastInstance instance = createHazelcastInstance();
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(instance);
        ss = nodeEngineImpl.getSerializationService();
        executionService = nodeEngineImpl.getExecutionService();
        properties = nodeEngineImpl.getProperties();
    }

    protected abstract NearCache<Integer, String> createNearCache(String name, NearCacheConfig nearCacheConfig,
                                                                  ManagedNearCacheRecordStore nearCacheRecordStore);

    protected NearCache<Integer, String> createNearCache(String name, ManagedNearCacheRecordStore nearCacheRecordStore) {
        return createNearCache(name, createNearCacheConfig(name, DEFAULT_MEMORY_FORMAT), nearCacheRecordStore);
    }

    protected Map<Integer, String> generateRandomKeyValueMappings() {
        Map<Integer, String> expectedKeyValueMappings = new HashMap<>();
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            expectedKeyValueMappings.put(i, "Record-" + i);
        }
        return expectedKeyValueMappings;
    }

    protected ManagedNearCacheRecordStore createManagedNearCacheRecordStore(
            Map<Integer, String> expectedKeyValueMappings) {
        return new ManagedNearCacheRecordStore(expectedKeyValueMappings);
    }

    protected ManagedNearCacheRecordStore createManagedNearCacheRecordStore() {
        return new ManagedNearCacheRecordStore(generateRandomKeyValueMappings());
    }

    protected void doGetNearCacheName() {
        NearCache<Integer, String> nearCache = createNearCache(DEFAULT_NEAR_CACHE_NAME, createManagedNearCacheRecordStore());

        assertEquals(DEFAULT_NEAR_CACHE_NAME, nearCache.getName());
    }

    protected void doGetFromNearCache() {
        Map<Integer, String> expectedKeyValueMappings = generateRandomKeyValueMappings();
        ManagedNearCacheRecordStore managedNearCacheRecordStore = createManagedNearCacheRecordStore(expectedKeyValueMappings);
        NearCache<Integer, String> nearCache = createNearCache(DEFAULT_NEAR_CACHE_NAME, managedNearCacheRecordStore);

        // show that NearCache delegates get call to wrapped NearCacheRecordStore
        assertEquals(nearCache.size(), managedNearCacheRecordStore.latestSize);

        for (int i = 0; i < expectedKeyValueMappings.size(); i++) {
            String value = nearCache.get(i);
            assertEquals((Integer) i, managedNearCacheRecordStore.latestKeyOnGet);
            assertEquals(value, managedNearCacheRecordStore.latestValueOnGet);
        }
    }

    protected void doPutToNearCache() {
        Map<Integer, String> expectedKeyValueMappings = new HashMap<>();
        ManagedNearCacheRecordStore managedNearCacheRecordStore = createManagedNearCacheRecordStore(expectedKeyValueMappings);
        NearCache<Integer, String> nearCache = createNearCache(DEFAULT_NEAR_CACHE_NAME, managedNearCacheRecordStore);

        // show that NearCache delegates put call to wrapped NearCacheRecordStore
        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            String value = "Record-" + i;
            nearCache.put(i, null, value, null);
            assertEquals((Integer) i, managedNearCacheRecordStore.latestKeyOnPut);
            assertEquals(value, managedNearCacheRecordStore.latestValueOnPut);
        }

        assertEquals(nearCache.size(), managedNearCacheRecordStore.latestSize);
    }

    protected void doRemoveFromNearCache() {
        Map<Integer, String> expectedKeyValueMappings = generateRandomKeyValueMappings();
        ManagedNearCacheRecordStore managedNearCacheRecordStore = createManagedNearCacheRecordStore(expectedKeyValueMappings);
        NearCache<Integer, String> nearCache = createNearCache(DEFAULT_NEAR_CACHE_NAME, managedNearCacheRecordStore);

        // show that NearCache delegates remove call to wrapped NearCacheRecordStore
        assertEquals(nearCache.size(), managedNearCacheRecordStore.latestSize);

        for (int i = 0; i < 2 * DEFAULT_RECORD_COUNT; i++) {
            nearCache.invalidate(i);
            assertEquals((Integer) i, managedNearCacheRecordStore.latestKeyOnRemove);
            assertEquals(i < DEFAULT_RECORD_COUNT, managedNearCacheRecordStore.latestResultOnRemove);
        }

        assertEquals(nearCache.size(), managedNearCacheRecordStore.latestSize);
    }

    protected void doInvalidateFromNearCache() {
        Map<Integer, String> expectedKeyValueMappings = generateRandomKeyValueMappings();
        ManagedNearCacheRecordStore managedNearCacheRecordStore = createManagedNearCacheRecordStore(expectedKeyValueMappings);
        NearCache<Integer, String> nearCache = createNearCache(DEFAULT_NEAR_CACHE_NAME, managedNearCacheRecordStore);

        // show that NearCache delegates invalidate call to wrapped NearCacheRecordStore
        assertEquals(nearCache.size(), managedNearCacheRecordStore.latestSize);

        for (int i = 0; i < 2 * DEFAULT_RECORD_COUNT; i++) {
            nearCache.invalidate(i);
            assertEquals((Integer) i, managedNearCacheRecordStore.latestKeyOnRemove);
            assertEquals(i < DEFAULT_RECORD_COUNT, managedNearCacheRecordStore.latestResultOnRemove);
        }

        assertEquals(nearCache.size(), managedNearCacheRecordStore.latestSize);
    }

    protected void doClearNearCache() {
        ManagedNearCacheRecordStore managedNearCacheRecordStore = createManagedNearCacheRecordStore();
        NearCache<Integer, String> nearCache = createNearCache(DEFAULT_NEAR_CACHE_NAME, managedNearCacheRecordStore);

        assertFalse(managedNearCacheRecordStore.clearCalled);
        assertEquals(nearCache.size(), managedNearCacheRecordStore.latestSize);

        nearCache.clear();

        // show that NearCache delegates clear call to wrapped NearCacheRecordStore
        assertTrue(managedNearCacheRecordStore.clearCalled);
        assertEquals(nearCache.size(), managedNearCacheRecordStore.latestSize);
    }

    protected void doDestroyNearCache() {
        ManagedNearCacheRecordStore managedNearCacheRecordStore = createManagedNearCacheRecordStore();
        NearCache<Integer, String> nearCache = createNearCache(DEFAULT_NEAR_CACHE_NAME, managedNearCacheRecordStore);

        assertFalse(managedNearCacheRecordStore.destroyCalled);

        nearCache.destroy();

        // show that NearCache delegates destroy call to wrapped NearCacheRecordStore
        assertTrue(managedNearCacheRecordStore.destroyCalled);
    }

    protected void doConfigureInMemoryFormatForNearCache() {
        NearCacheConfig config1 = createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME + "-1", DEFAULT_MEMORY_FORMAT);
        NearCacheConfig config2 = createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME + "-2", DEFAULT_MEMORY_FORMAT);

        config1.setInMemoryFormat(InMemoryFormat.OBJECT);
        config2.setInMemoryFormat(InMemoryFormat.BINARY);

        // show that NearCache gets "inMemoryFormat" configuration from specified NearCacheConfig
        assertEquals(InMemoryFormat.OBJECT, config1.getInMemoryFormat());
        assertEquals(InMemoryFormat.BINARY, config2.getInMemoryFormat());
    }

    protected void doGetNearCacheStatsFromNearCache() {
        ManagedNearCacheRecordStore managedNearCacheRecordStore = createManagedNearCacheRecordStore();
        NearCache<Integer, String> nearCache = createNearCache(DEFAULT_NEAR_CACHE_NAME, managedNearCacheRecordStore);

        // show that NearCache gets NearCacheStats from specified NearCacheRecordStore
        assertEquals(managedNearCacheRecordStore.getNearCacheStats(), nearCache.getNearCacheStats());
    }

    protected void doCreateNearCacheAndWaitForExpirationCalled(boolean useTTL) {
        final ManagedNearCacheRecordStore managedNearCacheRecordStore = createManagedNearCacheRecordStore();

        NearCacheConfig nearCacheConfig = createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, DEFAULT_MEMORY_FORMAT);
        if (useTTL) {
            nearCacheConfig.setTimeToLiveSeconds(DEFAULT_EXPIRATION_TASK_INITIAL_DELAY_SECONDS - 1);
        } else {
            nearCacheConfig.setMaxIdleSeconds(DEFAULT_EXPIRATION_TASK_INITIAL_DELAY_SECONDS - 1);
        }

        createNearCache(DEFAULT_NEAR_CACHE_NAME, nearCacheConfig, managedNearCacheRecordStore).initialize();

        sleepSeconds(DEFAULT_EXPIRATION_TASK_INITIAL_DELAY_SECONDS + 1);

        // expiration will be called eventually
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(managedNearCacheRecordStore.doExpirationCalled);
            }
        });
    }

    protected void doPutToNearCacheStatsAndSeeEvictionCheckIsDone() {
        ManagedNearCacheRecordStore managedNearCacheRecordStore = createManagedNearCacheRecordStore();
        NearCache<Integer, String> nearCache = createNearCache(DEFAULT_NEAR_CACHE_NAME, managedNearCacheRecordStore);

        nearCache.put(1, null, "1", null);

        // show that NearCache checks eviction from specified NearCacheRecordStore
        assertTrue(managedNearCacheRecordStore.doEvictionIfRequiredCalled);
    }

    protected static class ManagedNearCacheRecordStore implements NearCacheRecordStore<Integer, String> {

        protected final NearCacheStats nearCacheStats = new NearCacheStatsImpl();

        protected Map<Integer, String> expectedKeyValueMappings;
        protected Integer latestKeyOnGet;
        protected String latestValueOnGet;
        protected Integer latestKeyOnPut;
        protected String latestValueOnPut;
        protected Integer latestKeyOnRemove;
        protected int latestSize;
        protected boolean latestResultOnRemove;

        protected volatile boolean clearCalled;
        protected volatile boolean destroyCalled;
        protected volatile boolean doEvictionIfRequiredCalled;
        protected volatile boolean doExpirationCalled;

        protected volatile StaleReadDetector staleReadDetector = StaleReadDetector.ALWAYS_FRESH;
        protected final AtomicLong reservationIdGenerator = new AtomicLong();

        protected ManagedNearCacheRecordStore(Map<Integer, String> expectedKeyValueMappings) {
            this.expectedKeyValueMappings = expectedKeyValueMappings;
        }

        @Override
        public void initialize() {
        }

        @Override
        public String get(Integer key) {
            if (expectedKeyValueMappings == null) {
                throw new IllegalStateException("Near Cache is already destroyed");
            }
            String value = expectedKeyValueMappings.get(key);
            latestKeyOnGet = key;
            latestValueOnGet = value;
            return value;
        }

        @Override
        public NearCacheRecord getRecord(Integer key) {
            return null;
        }

        @Override
        public void put(Integer key, Data keyData, String value, Data valueData) {
            long reservationId = tryReserveForUpdate(key, keyData, READ_UPDATE);
            if (reservationId != NOT_RESERVED) {
                tryPublishReserved(key, value, reservationId, false);
            }
        }

        @Override
        public void invalidate(Integer key) {
            if (expectedKeyValueMappings == null) {
                throw new IllegalStateException("Near Cache is already destroyed");
            }
            boolean result = expectedKeyValueMappings.remove(key) != null;
            latestKeyOnRemove = key;
            latestResultOnRemove = result;
        }

        @Override
        public void clear() {
            if (expectedKeyValueMappings == null) {
                throw new IllegalStateException("Near Cache is already destroyed");
            }
            expectedKeyValueMappings.clear();
            clearCalled = true;
        }

        @Override
        public void destroy() {
            if (expectedKeyValueMappings == null) {
                throw new IllegalStateException("Near Cache is already destroyed");
            }
            expectedKeyValueMappings.clear();
            expectedKeyValueMappings = null;
            destroyCalled = true;
        }

        @Override
        public NearCacheStats getNearCacheStats() {
            return nearCacheStats;
        }

        @Override
        public int size() {
            if (expectedKeyValueMappings == null) {
                throw new IllegalStateException("Near Cache is already destroyed");
            }
            latestSize = expectedKeyValueMappings.size();
            return latestSize;
        }

        @Override
        public void doExpiration() {
            if (expectedKeyValueMappings == null) {
                throw new IllegalStateException("Near Cache is already destroyed");
            }
            doExpirationCalled = true;
        }

        @Override
        public boolean doEviction(boolean withoutMaxSizeCheck) {
            if (expectedKeyValueMappings == null) {
                throw new IllegalStateException("Near Cache is already destroyed");
            }
            doEvictionIfRequiredCalled = true;
            return true;
        }

        @Override
        public void storeKeys() {
        }

        @Override
        public void loadKeys(DataStructureAdapter adapter) {
        }

        @Override
        public void setStaleReadDetector(StaleReadDetector detector) {
            staleReadDetector = detector;
        }

        @Override
        public long tryReserveForUpdate(Integer key, Data keyData, NearCache.UpdateSemantic updateSemantic) {
            return reservationIdGenerator.incrementAndGet();
        }

        @Override
        public String tryPublishReserved(Integer key, String value, long reservationId, boolean deserialize) {
            if (expectedKeyValueMappings == null) {
                throw new IllegalStateException("Near Cache is already destroyed");
            }
            expectedKeyValueMappings.put(key, value);
            latestKeyOnPut = key;
            latestValueOnPut = value;
            return value;
        }
    }
}
