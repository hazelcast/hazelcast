package com.hazelcast.cache.nearcache;

import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.cache.impl.nearcache.NearCacheRecordStore;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.monitor.impl.NearCacheStatsImpl;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class NearCacheTestSupport extends CommonNearCacheTestSupport {

    protected abstract NearCache<Integer, String> createNearCache(String name,
                                                                  NearCacheConfig nearCacheConfig,
                                                                  ManagedNearCacheRecordStore nearCacheRecordStore);

    protected NearCache<Integer, String> createNearCache(String name,
                                                         ManagedNearCacheRecordStore nearCacheRecordStore) {
        return createNearCache(name,
                               createNearCacheConfig(name, NearCacheConfig.DEFAULT_MEMORY_FORMAT),
                               nearCacheRecordStore);
    }

    protected class ManagedNearCacheRecordStore implements NearCacheRecordStore<Integer, String> {

        protected final NearCacheStats nearCacheStats = new NearCacheStatsImpl();

        protected Map<Integer, String> expectedKeyValueMappings;
        protected Integer latestKeyOnGet;
        protected String latestValueOnGet;
        protected Integer latestKeyOnPut;
        protected String latestValueOnPut;
        protected Integer latestKeyOnRemove;
        protected boolean latestResultOnRemove;
        protected boolean clearCalled;
        protected boolean destroyCalled;
        protected boolean selectToSaveCalled;
        protected final Object selectedCandidateToSave = new Object();

        protected ManagedNearCacheRecordStore(Map<Integer, String> expectedKeyValueMappings) {
            this.expectedKeyValueMappings = expectedKeyValueMappings;
        }

        @Override
        public String get(Integer key) {
            String value = expectedKeyValueMappings.get(key);
            latestKeyOnGet = key;
            latestValueOnGet = value;
            return value;
        }

        @Override
        public void put(Integer key, String value) {
            expectedKeyValueMappings.put(key, value);
            latestKeyOnPut = key;
            latestValueOnPut = value;
        }

        @Override
        public boolean remove(Integer key) {
            boolean result = expectedKeyValueMappings.remove(key) != null;
            latestKeyOnRemove = key;
            latestResultOnRemove = result;
            return result;
        }

        @Override
        public void clear() {
            expectedKeyValueMappings.clear();
            clearCalled = true;
        }

        @Override
        public void destroy() {
            expectedKeyValueMappings.clear();
            expectedKeyValueMappings = null;
            destroyCalled = true;
        }

        @Override
        public NearCacheStats getNearCacheStats() {
            return nearCacheStats;
        }

        @Override
        public Object selectToSave(Object... candidates) {
            selectToSaveCalled = true;
            return selectedCandidateToSave;
        }

    }

    protected Map<Integer, String> generateRandomKeyValueMappings() {
        Map<Integer, String> expectedKeyValueMappings = new HashMap<Integer, String>();
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
        NearCache<Integer, String> nearCache =
                createNearCache(DEFAULT_NEAR_CACHE_NAME, createManagedNearCacheRecordStore());

        assertEquals(DEFAULT_NEAR_CACHE_NAME, nearCache.getName());
    }

    protected void doGetFromNearCache() {
        Map<Integer, String> expectedKeyValueMappings = generateRandomKeyValueMappings();
        ManagedNearCacheRecordStore managedNearCacheRecordStore =
                createManagedNearCacheRecordStore(expectedKeyValueMappings);
        NearCache<Integer, String> nearCache =
                createNearCache(DEFAULT_NEAR_CACHE_NAME, managedNearCacheRecordStore);

        // Show that NearCache delegates get call to wrapped NearCacheRecordStore

        for (int i = 0; i < expectedKeyValueMappings.size(); i++) {
            String value = nearCache.get(i);
            assertEquals((Integer) i, managedNearCacheRecordStore.latestKeyOnGet);
            assertEquals(value, managedNearCacheRecordStore.latestValueOnGet);
        }
    }

    protected void doPutFromNearCache() {
        Map<Integer, String> expectedKeyValueMappings = new HashMap<Integer, String>();
        ManagedNearCacheRecordStore managedNearCacheRecordStore =
                createManagedNearCacheRecordStore(expectedKeyValueMappings);
        NearCache<Integer, String> nearCache =
                createNearCache(DEFAULT_NEAR_CACHE_NAME, managedNearCacheRecordStore);

        // Show that NearCache delegates put call to wrapped NearCacheRecordStore

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            String value = "Record-" + i;
            nearCache.put(i, value);
            assertEquals((Integer) i, managedNearCacheRecordStore.latestKeyOnPut);
            assertEquals(value, managedNearCacheRecordStore.latestValueOnPut);
        }
    }

    protected void doRemoveFromNearCache() {
        Map<Integer, String> expectedKeyValueMappings = generateRandomKeyValueMappings();
        ManagedNearCacheRecordStore managedNearCacheRecordStore =
                createManagedNearCacheRecordStore(expectedKeyValueMappings);
        NearCache<Integer, String> nearCache =
                createNearCache(DEFAULT_NEAR_CACHE_NAME, managedNearCacheRecordStore);

        // Show that NearCache delegates remove call to wrapped NearCacheRecordStore

        for (int i = 0; i < 2 * DEFAULT_RECORD_COUNT; i++) {
            nearCache.remove(i);
            assertEquals((Integer) i, managedNearCacheRecordStore.latestKeyOnRemove);
            assertEquals(i < DEFAULT_RECORD_COUNT, managedNearCacheRecordStore.latestResultOnRemove);
        }
    }

    protected void doInvalidateFromNearCache() {
        Map<Integer, String> expectedKeyValueMappings = generateRandomKeyValueMappings();
        ManagedNearCacheRecordStore managedNearCacheRecordStore =
                createManagedNearCacheRecordStore(expectedKeyValueMappings);
        NearCache<Integer, String> nearCache =
                createNearCache(DEFAULT_NEAR_CACHE_NAME, managedNearCacheRecordStore);

        // Show that NearCache delegates invalidate call to wrapped NearCacheRecordStore

        for (int i = 0; i < 2 * DEFAULT_RECORD_COUNT; i++) {
            nearCache.invalidate(i);
            assertEquals((Integer) i, managedNearCacheRecordStore.latestKeyOnRemove);
            assertEquals(i < DEFAULT_RECORD_COUNT, managedNearCacheRecordStore.latestResultOnRemove);
        }
    }

    protected void doConfigureInvalidateOnChangeForNearCache() {
        NearCacheConfig config1 =
                createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME + "-1",
                        NearCacheConfig.DEFAULT_MEMORY_FORMAT);
        NearCacheConfig config2 =
                createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME + "-2",
                        NearCacheConfig.DEFAULT_MEMORY_FORMAT);

        config1.setInvalidateOnChange(false);
        config2.setInvalidateOnChange(true);

        NearCache nearCache1 =
                createNearCache(config1.getName(), config1, createManagedNearCacheRecordStore());
        NearCache nearCache2 =
                createNearCache(config2.getName(), config2, createManagedNearCacheRecordStore());

        // Show that NearCache gets "isInvalidateOnChange" configuration from specified NearCacheConfig

        assertFalse(nearCache1.isInvalidateOnChange());
        assertTrue(nearCache2.isInvalidateOnChange());
    }

    protected void doClearNearCache() {
        ManagedNearCacheRecordStore managedNearCacheRecordStore =
                createManagedNearCacheRecordStore();
        NearCache<Integer, String> nearCache =
                createNearCache(DEFAULT_NEAR_CACHE_NAME, managedNearCacheRecordStore);

        assertFalse(managedNearCacheRecordStore.clearCalled);

        nearCache.clear();

        // Show that NearCache delegates clear call to wrapped NearCacheRecordStore

        assertTrue(managedNearCacheRecordStore.clearCalled);
    }

    protected void doDestroyNearCache() {
        ManagedNearCacheRecordStore managedNearCacheRecordStore =
                createManagedNearCacheRecordStore();
        NearCache<Integer, String> nearCache =
                createNearCache(DEFAULT_NEAR_CACHE_NAME, managedNearCacheRecordStore);

        assertFalse(managedNearCacheRecordStore.destroyCalled);

        nearCache.destroy();

        // Show that NearCache delegates destroy call to wrapped NearCacheRecordStore

        assertTrue(managedNearCacheRecordStore.destroyCalled);
    }

    protected void doConfigureInMemoryFormatForNearCache() {
        NearCacheConfig config1 =
                createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME + "-1",
                        NearCacheConfig.DEFAULT_MEMORY_FORMAT);
        NearCacheConfig config2 =
                createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME + "-2",
                        NearCacheConfig.DEFAULT_MEMORY_FORMAT);

        config1.setInMemoryFormat(InMemoryFormat.OBJECT);
        config2.setInMemoryFormat(InMemoryFormat.BINARY);

        NearCache nearCache1 =
                createNearCache(config1.getName(), config1, createManagedNearCacheRecordStore());
        NearCache nearCache2 =
                createNearCache(config2.getName(), config2, createManagedNearCacheRecordStore());

        // Show that NearCache gets "inMemoryFormat" configuration from specified NearCacheConfig

        assertEquals(InMemoryFormat.OBJECT, nearCache1.getInMemoryFormat());
        assertEquals(InMemoryFormat.BINARY, nearCache2.getInMemoryFormat());
    }

    protected void doGetNearCacheStatsFromNearCache() {
        ManagedNearCacheRecordStore managedNearCacheRecordStore =
                createManagedNearCacheRecordStore();
        NearCache<Integer, String> nearCache =
                createNearCache(DEFAULT_NEAR_CACHE_NAME, managedNearCacheRecordStore);

        // Show that NearCache gets NearCacheStats from specified NearCacheRecordStore

        assertEquals(managedNearCacheRecordStore.getNearCacheStats(), nearCache.getNearCacheStats());
    }

    protected void doSelectToSaveFromNearCache() {
        ManagedNearCacheRecordStore managedNearCacheRecordStore =
                createManagedNearCacheRecordStore();
        NearCache<Integer, String> nearCache =
                createNearCache(DEFAULT_NEAR_CACHE_NAME, managedNearCacheRecordStore);

        Object selectedCandidate = nearCache.selectToSave();

        // Show that NearCache gets selected candidate from specified NearCacheRecordStore

        assertTrue(managedNearCacheRecordStore.selectToSaveCalled);
        assertEquals(managedNearCacheRecordStore.selectedCandidateToSave, selectedCandidate);
    }

}
