package com.hazelcast.cache.nearcache;

import com.hazelcast.cache.impl.nearcache.NearCacheRecordStore;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class NearCacheRecordStoreTest extends NearCacheRecordStoreTestSupport {

    @Test
    public void putAndGetRecordFromNearCacheObjectRecordStore() {
        putAndGetRecord(InMemoryFormat.OBJECT);
    }

    @Test
    public void putAndGetRecordSuccessfullyFromNearCacheDataRecordStore() {
        putAndGetRecord(InMemoryFormat.BINARY);
    }

    @Test
    public void putAndRemoveRecordFromNearCacheObjectRecordStore() {
        putAndRemoveRecord(InMemoryFormat.OBJECT);
    }

    @Test
    public void putAndRemoveRecordSuccessfullyFromNearCacheDataRecordStore() {
        putAndRemoveRecord(InMemoryFormat.BINARY);
    }

    @Test
    public void clearRecordsFromNearCacheObjectRecordStore() {
        clearRecordsOrDestroyStoreFromNearCacheDataRecordStore(InMemoryFormat.OBJECT, false);
    }

    @Test
    public void clearRecordsSuccessfullyFromNearCacheDataRecordStore() {
        clearRecordsOrDestroyStoreFromNearCacheDataRecordStore(InMemoryFormat.BINARY, false);
    }

    @Test(expected = IllegalStateException.class)
    public void destroyStoreFromNearCacheObjectRecordStore() {
        clearRecordsOrDestroyStoreFromNearCacheDataRecordStore(InMemoryFormat.OBJECT, true);
    }

    @Test(expected = IllegalStateException.class)
    public void destroyStoreFromNearCacheDataRecordStore() {
        clearRecordsOrDestroyStoreFromNearCacheDataRecordStore(InMemoryFormat.BINARY, true);
    }

    @Test
    public void statsCalculatedOnNearCacheObjectRecordStore() {
        statsCalculated(InMemoryFormat.OBJECT);
    }

    @Test
    public void statsCalculatedOnNearCacheDataRecordStore() {
        statsCalculated(InMemoryFormat.BINARY);
    }

    @Test
    public void ttlEvaluatedOnNearCacheObjectRecordStore() {
        ttlEvaluated(InMemoryFormat.OBJECT);
    }

    @Test
    public void ttlEvaluatedSuccessfullyOnNearCacheDataRecordStore() {
        ttlEvaluated(InMemoryFormat.BINARY);
    }

    @Test
    public void maxIdleTimeEvaluatedSuccessfullyOnNearCacheObjectRecordStore() {
        maxIdleTimeEvaluatedSuccessfully(InMemoryFormat.OBJECT);
    }

    @Test
    public void maxIdleTimeEvaluatedSuccessfullyOnNearCacheDataRecordStore() {
        maxIdleTimeEvaluatedSuccessfully(InMemoryFormat.BINARY);
    }

    @Test
    public void expiredRecordsCleanedUpSuccessfullyBecauseOfTTLOnNearCacheObjectRecordStore() {
        expiredRecordsCleanedUpSuccessfully(InMemoryFormat.OBJECT, false);
    }

    @Test
    public void expiredRecordsCleanedUpSuccessfullyBecauseOfTTLOnNearCacheDataRecordStore() {
        expiredRecordsCleanedUpSuccessfully(InMemoryFormat.BINARY, false);
    }

    @Test
    public void expiredRecordsCleanedUpSuccessfullyBecauseOfIdleTimeOnNearCacheObjectRecordStore() {
        expiredRecordsCleanedUpSuccessfully(InMemoryFormat.OBJECT, true);
    }

    @Test
    public void expiredRecordsCleanedUpSuccessfullyBecauseOfIdleTimeOnNearCacheDataRecordStore() {
        expiredRecordsCleanedUpSuccessfully(InMemoryFormat.BINARY, true);
    }

    @Test
    public void canCreateNearCacheObjectRecordStoreWithEntryCountMaxSizePolicy() {
        createNearCacheWithMaxSizePolicy(InMemoryFormat.OBJECT,
                EvictionConfig.MaxSizePolicy.ENTRY_COUNT,
                1000);
    }

    @Test(expected = IllegalArgumentException.class)
    public void cannotCreateNearCacheObjectRecordStoreWithUsedNativeMemorySizeMaxSizePolicy() {
        createNearCacheWithMaxSizePolicy(InMemoryFormat.OBJECT,
                EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_SIZE,
                1000000);
    }

    @Test(expected = IllegalArgumentException.class)
    public void cannotCreateNearCacheObjectRecordStoreWithFreeNativeMemorySizeMaxSizePolicy() {
        createNearCacheWithMaxSizePolicy(InMemoryFormat.OBJECT,
                EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE,
                1000000);
    }

    @Test(expected = IllegalArgumentException.class)
    public void cannotCreateNearCacheObjectRecordStoreWithUsedNativeMemoryPercentageMaxSizePolicy() {
        createNearCacheWithMaxSizePolicy(InMemoryFormat.OBJECT,
                EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE,
                99);
    }

    @Test(expected = IllegalArgumentException.class)
    public void cannotCreateNearCacheObjectRecordStoreWithFreeNativeMemoryPercentageMaxSizePolicy() {
        createNearCacheWithMaxSizePolicy(InMemoryFormat.OBJECT,
                EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE,
                1);
    }

    @Test
    public void canCreateNearCacheDataRecordStoreWithEntryCountMaxSizePolicy() {
        createNearCacheWithMaxSizePolicy(InMemoryFormat.BINARY,
                EvictionConfig.MaxSizePolicy.ENTRY_COUNT,
                1000);
    }

    @Test(expected = IllegalArgumentException.class)
    public void cannotCreateNearCacheDataRecordStoreWithUsedNativeMemorySizeMaxSizePolicy() {
        createNearCacheWithMaxSizePolicy(InMemoryFormat.BINARY,
                EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_SIZE,
                1000000);
    }

    @Test(expected = IllegalArgumentException.class)
    public void cannotCreateNearCacheDataRecordStoreWithFreeNativeMemorySizeMaxSizePolicy() {
        createNearCacheWithMaxSizePolicy(InMemoryFormat.BINARY,
                EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE,
                1000000);
    }

    @Test(expected = IllegalArgumentException.class)
    public void cannotCreateNearCacheDataRecordStoreWithUsedNativeMemoryPercentageMaxSizePolicy() {
        createNearCacheWithMaxSizePolicy(InMemoryFormat.BINARY,
                EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE,
                99);
    }

    @Test(expected = IllegalArgumentException.class)
    public void cannotCreateNearCacheDataRecordStoreWithFreeNativeMemoryPercentageMaxSizePolicy() {
        createNearCacheWithMaxSizePolicy(InMemoryFormat.BINARY,
                EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE,
                1);
    }

    private void doEvictionWithEntryCountMaxSizePolicy(InMemoryFormat inMemoryFormat,
                                                       EvictionPolicy evictionPolicy) {
        final int MAX_SIZE = DEFAULT_RECORD_COUNT / 2;

        NearCacheConfig nearCacheConfig =
                createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, inMemoryFormat);

        if (evictionPolicy == null) {
            evictionPolicy = EvictionConfig.DEFAULT_EVICTION_POLICY;
        }
        EvictionConfig evictionConfig = new EvictionConfig();
        evictionConfig.setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.ENTRY_COUNT);
        evictionConfig.setSize(MAX_SIZE);
        evictionConfig.setEvictionPolicy(evictionPolicy);
        nearCacheConfig.setEvictionConfig(evictionConfig);

        NearCacheRecordStore<Integer, String> nearCacheRecordStore =
                createNearCacheRecordStore(
                        nearCacheConfig,
                        createNearCacheContext(),
                        inMemoryFormat);

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            nearCacheRecordStore.put(i, "Record-" + i);
            nearCacheRecordStore.doEvictionIfRequired();
            assertTrue(MAX_SIZE >= nearCacheRecordStore.size());
        }
    }

    @Test
    public void evictionTriggeredAndHandledSuccessfullyWithEntryCountMaxSizePolicyAndLRUEvictionPolicyOnNearCacheObjectRecordStore() {
        doEvictionWithEntryCountMaxSizePolicy(InMemoryFormat.OBJECT, EvictionPolicy.LRU);
    }

    @Test
    public void evictionTriggeredAndHandledSuccessfullyWithEntryCountMaxSizePolicyAndLFUEvictionPolicyOnNearCacheObjectRecordStore() {
        doEvictionWithEntryCountMaxSizePolicy(InMemoryFormat.OBJECT, EvictionPolicy.LFU);
    }

    @Test
    public void evictionTriggeredAndHandledSuccessfullyWithEntryCountMaxSizePolicyAndDefaultEvictionPolicyOnNearCacheObjectRecordStore() {
        doEvictionWithEntryCountMaxSizePolicy(InMemoryFormat.OBJECT, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void evictionNotSupportedWithEntryCountMaxSizePolicyAndRandomEvictionPolicyNearCacheObjectRecordStore() {
        doEvictionWithEntryCountMaxSizePolicy(InMemoryFormat.OBJECT, EvictionPolicy.RANDOM);
    }

    @Test
    public void evictionTriggeredAndHandledSuccessfullyWithEntryCountMaxSizePolicyAndLRUEvictionPolicyNearCacheDataRecordStore() {
        doEvictionWithEntryCountMaxSizePolicy(InMemoryFormat.BINARY, EvictionPolicy.LRU);
    }

    @Test
    public void evictionTriggeredAndHandledSuccessfullyWithEntryCountMaxSizePolicyAndLFUEvictionPolicyNearCacheDataRecordStore() {
        doEvictionWithEntryCountMaxSizePolicy(InMemoryFormat.BINARY, EvictionPolicy.LFU);
    }

    @Test
    public void evictionTriggeredAndHandledSuccessfullyWithEntryCountMaxSizePolicyAndDefaultEvictionPolicyNearCacheDataRecordStore() {
        doEvictionWithEntryCountMaxSizePolicy(InMemoryFormat.BINARY, EvictionPolicy.LFU);
    }

    @Test(expected = IllegalArgumentException.class)
    public void evictionNotSupportedWithEntryCountMaxSizePolicyAndRandomEvictionPolicyNearCacheDataRecordStore() {
        doEvictionWithEntryCountMaxSizePolicy(InMemoryFormat.BINARY, EvictionPolicy.RANDOM);
    }

}
