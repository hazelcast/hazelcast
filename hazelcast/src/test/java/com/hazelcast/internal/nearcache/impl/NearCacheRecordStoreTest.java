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

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.internal.nearcache.NearCacheRecordStore;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NearCacheRecordStoreTest extends NearCacheRecordStoreTestSupport {

    @Parameters(name = "format:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.BINARY},
                {InMemoryFormat.OBJECT},
        });
    }

    @Parameter
    public InMemoryFormat inMemoryFormat;

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

    @Test
    public void destroyStore() {
        clearRecordsOrDestroyStore(inMemoryFormat, true);
    }

    @Test
    public void statsCalculated() {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                statsCalculated(inMemoryFormat);
            }
        });
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

        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaxSizePolicy(MaxSizePolicy.ENTRY_COUNT)
                .setSize(maxSize)
                .setEvictionPolicy(evictionPolicy == null ? EvictionConfig.DEFAULT_EVICTION_POLICY : evictionPolicy);

        NearCacheConfig nearCacheConfig = createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, inMemoryFormat)
                .setEvictionConfig(evictionConfig);

        NearCacheRecordStore<Integer, String> nearCacheRecordStore = createNearCacheRecordStore(nearCacheConfig, inMemoryFormat);

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            nearCacheRecordStore.put(i, null, "Record-" + i, null);
            nearCacheRecordStore.doEviction(false);
            assertTrue(maxSize >= nearCacheRecordStore.size());
        }
    }
}
