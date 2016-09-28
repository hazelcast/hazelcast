package com.hazelcast.cache.nearcache;

import com.hazelcast.cache.impl.nearcache.NearCacheRecordStore;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionConfig.MaxSizePolicy;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category(QuickTest.class)
public class NearCacheRecordStoreTest extends NearCacheRecordStoreTestSupport {

    @Parameterized.Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameterized.Parameters(name = "format:{0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {InMemoryFormat.BINARY},
                {InMemoryFormat.OBJECT},
        });
    }

    @Test
    public void putAndGetRecord() {
        putAndGetRecord(inMemoryFormat);
    }

    @Test
    public void putAndRemoveRecord() {
        putAndRemoveRecord(inMemoryFormat);
    }

    @Test
    public void clearRecords() {
        clearRecordsOrDestroyStore(inMemoryFormat, false);
    }

    @Test(expected = IllegalStateException.class)
    public void destroyStore() {
        clearRecordsOrDestroyStore(inMemoryFormat, true);
    }

    @Test
    public void statsCalculated() {
        statsCalculated(inMemoryFormat);
    }

    @Test
    public void ttlEvaluated() {
        ttlEvaluated(inMemoryFormat);
    }

    @Test
    public void maxIdleTimeEvaluatedSuccessfully() {
        maxIdleTimeEvaluatedSuccessfully(inMemoryFormat);
    }

    @Test
    public void expiredRecordsCleanedUpSuccessfullyBecauseOfTTL() {
        expiredRecordsCleanedUpSuccessfully(inMemoryFormat, false);
    }

    @Test
    public void expiredRecordsCleanedUpSuccessfullyBecauseOfIdleTime() {
        expiredRecordsCleanedUpSuccessfully(inMemoryFormat, true);
    }

    @Test
    public void canCreateWithEntryCountMaxSizePolicy() {
        createNearCacheWithMaxSizePolicy(inMemoryFormat, MaxSizePolicy.ENTRY_COUNT, 1000);
    }

    @Test(expected = IllegalArgumentException.class)
    public void cannotCreateWithUsedNativeMemorySizeMaxSizePolicy() {
        createNearCacheWithMaxSizePolicy(inMemoryFormat, MaxSizePolicy.USED_NATIVE_MEMORY_SIZE, 1000000);
    }

    @Test(expected = IllegalArgumentException.class)
    public void cannotCreateWithFreeNativeMemorySizeMaxSizePolicy() {
        createNearCacheWithMaxSizePolicy(inMemoryFormat, MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE, 1000000);
    }

    @Test(expected = IllegalArgumentException.class)
    public void cannotCreateWithUsedNativeMemoryPercentageMaxSizePolicy() {
        createNearCacheWithMaxSizePolicy(inMemoryFormat, MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE, 99);
    }

    @Test(expected = IllegalArgumentException.class)
    public void cannotCreateNearWithFreeNativeMemoryPercentageMaxSizePolicy() {
        createNearCacheWithMaxSizePolicy(inMemoryFormat, MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE, 1);
    }

    @Test
    public void evictionTriggeredAndHandledSuccessfullyWithEntryCountMaxSizePolicyAndLRUEvictionPolicy() {
        doEvictionWithEntryCountMaxSizePolicy(inMemoryFormat, EvictionPolicy.LRU);
    }

    @Test
    public void evictionTriggeredAndHandledSuccessfullyWithEntryCountMaxSizePolicyAndLFUEvictionPolicy() {
        doEvictionWithEntryCountMaxSizePolicy(inMemoryFormat, EvictionPolicy.LFU);
    }

    @Test
    public void evictionTriggeredAndHandledSuccessfullyWithEntryCountMaxSizePolicyAndRandomEvictionPolicy() {
        doEvictionWithEntryCountMaxSizePolicy(inMemoryFormat, EvictionPolicy.RANDOM);
    }

    @Test
    public void evictionTriggeredAndHandledSuccessfullyWithEntryCountMaxSizePolicyAndDefaultEvictionPolicy() {
        doEvictionWithEntryCountMaxSizePolicy(inMemoryFormat, null);
    }

    private void doEvictionWithEntryCountMaxSizePolicy(InMemoryFormat inMemoryFormat, EvictionPolicy evictionPolicy) {
        int maxSize = DEFAULT_RECORD_COUNT / 2;

        NearCacheConfig nearCacheConfig = createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, inMemoryFormat);

        if (evictionPolicy == null) {
            evictionPolicy = EvictionConfig.DEFAULT_EVICTION_POLICY;
        }
        EvictionConfig evictionConfig = new EvictionConfig();
        evictionConfig.setMaximumSizePolicy(MaxSizePolicy.ENTRY_COUNT);
        evictionConfig.setSize(maxSize);
        evictionConfig.setEvictionPolicy(evictionPolicy);
        nearCacheConfig.setEvictionConfig(evictionConfig);

        NearCacheRecordStore<Integer, String> nearCacheRecordStore = createNearCacheRecordStore(
                nearCacheConfig,
                createNearCacheContext(),
                inMemoryFormat);

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            nearCacheRecordStore.put(i, "Record-" + i);
            nearCacheRecordStore.doEvictionIfRequired();
            assertTrue(maxSize >= nearCacheRecordStore.size());
        }
    }
}
