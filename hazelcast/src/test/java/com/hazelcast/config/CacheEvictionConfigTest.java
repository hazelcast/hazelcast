/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import com.hazelcast.config.CacheEvictionConfig.CacheMaxSizePolicy;
import com.hazelcast.config.EvictionConfig.MaxSizePolicy;
import com.hazelcast.internal.eviction.EvictableEntryView;
import com.hazelcast.internal.eviction.EvictionPolicyComparator;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.CacheEvictionConfig.CacheMaxSizePolicy.fromMaxSizePolicy;
import static com.hazelcast.config.EvictionPolicy.LFU;
import static com.hazelcast.config.EvictionPolicy.LRU;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CacheEvictionConfigTest {

    @Test
    public void test_cacheEvictionConfig_shouldInheritConstructors_from_evictionConfig_correctly() {
        CacheEvictionConfig cacheEvictionConfig1 = new CacheEvictionConfig(1000, MaxSizePolicy.ENTRY_COUNT, LFU);
        assertEquals(1000, cacheEvictionConfig1.getSize());
        assertEquals(MaxSizePolicy.ENTRY_COUNT, cacheEvictionConfig1.getMaximumSizePolicy());
        assertEquals(LFU, cacheEvictionConfig1.getEvictionPolicy());
        assertNotNull(cacheEvictionConfig1.toString());

        CacheEvictionConfig cacheEvictionConfig2 = new CacheEvictionConfig(1000, CacheMaxSizePolicy.ENTRY_COUNT, LFU);
        assertEquals(1000, cacheEvictionConfig2.getSize());
        assertEquals(CacheMaxSizePolicy.ENTRY_COUNT, cacheEvictionConfig2.getMaxSizePolicy());
        assertEquals(LFU, cacheEvictionConfig2.getEvictionPolicy());
        assertNotNull(cacheEvictionConfig2.toString());

        String comparatorName = "myComparator";

        CacheEvictionConfig cacheEvictionConfig3 = new CacheEvictionConfig(1000, MaxSizePolicy.ENTRY_COUNT, comparatorName);
        assertEquals(1000, cacheEvictionConfig3.getSize());
        assertEquals(MaxSizePolicy.ENTRY_COUNT, cacheEvictionConfig3.getMaximumSizePolicy());
        assertEquals(comparatorName, cacheEvictionConfig3.getComparatorClassName());
        assertNotNull(cacheEvictionConfig3.toString());

        CacheEvictionConfig cacheEvictionConfig4 = new CacheEvictionConfig(1000, CacheMaxSizePolicy.ENTRY_COUNT,
                comparatorName);
        assertEquals(1000, cacheEvictionConfig4.getSize());
        assertEquals(CacheMaxSizePolicy.ENTRY_COUNT, cacheEvictionConfig4.getMaxSizePolicy());
        assertEquals(comparatorName, cacheEvictionConfig4.getComparatorClassName());
        assertNotNull(cacheEvictionConfig4.toString());

        EvictionPolicyComparator comparator = new EvictionPolicyComparator() {
            @Override
            public int compare(EvictableEntryView e1, EvictableEntryView e2) {
                return 0;
            }
        };

        CacheEvictionConfig cacheEvictionConfig5 = new CacheEvictionConfig(1000, MaxSizePolicy.ENTRY_COUNT, comparator);
        assertEquals(1000, cacheEvictionConfig5.getSize());
        assertEquals(MaxSizePolicy.ENTRY_COUNT, cacheEvictionConfig5.getMaximumSizePolicy());
        assertEquals(comparator, cacheEvictionConfig5.getComparator());
        assertNotNull(cacheEvictionConfig5.toString());

        CacheEvictionConfig cacheEvictionConfig6 = new CacheEvictionConfig(1000, CacheMaxSizePolicy.ENTRY_COUNT, comparator);
        assertEquals(1000, cacheEvictionConfig6.getSize());
        assertEquals(CacheMaxSizePolicy.ENTRY_COUNT, cacheEvictionConfig6.getMaxSizePolicy());
        assertEquals(comparator, cacheEvictionConfig6.getComparator());
        assertNotNull(cacheEvictionConfig6.toString());
    }

    @Test
    public void cacheEvictionConfig_shouldInheritAttributes_from_evictionConfig_correctly() {
        CacheEvictionConfig cacheEvictionConfig = new CacheEvictionConfig();
        cacheEvictionConfig.setComparatorClassName("myComparator");
        cacheEvictionConfig.setEvictionPolicy(LRU);

        assertEquals("myComparator", cacheEvictionConfig.getComparatorClassName());
        assertEquals(LRU, cacheEvictionConfig.getEvictionPolicy());
        assertNotNull(cacheEvictionConfig.toString());

        CacheEvictionConfig cacheEvictionConfigReadOnly = cacheEvictionConfig.getAsReadOnly();

        assertNotNull(cacheEvictionConfigReadOnly);
        assertEquals("myComparator", cacheEvictionConfigReadOnly.getComparatorClassName());
        assertEquals(LRU, cacheEvictionConfigReadOnly.getEvictionPolicy());
        assertNotNull(cacheEvictionConfigReadOnly.toString());
    }

    @Test
    public void cacheEvictionConfig_shouldDelegate_maxSizePolicy_of_evictionConfig_when_maxSizePolicyIs_entryCount() {
        cacheEvictionConfig_shouldDelegate_maxSizePolicy_of_evictionConfig(MaxSizePolicy.ENTRY_COUNT);
    }

    @Test
    public void cacheEvictionConfig_shouldDelegate_maxSizePolicy_of_evictionConfig_when_maxSizePolicyIs_usedNativeMemorySize() {
        cacheEvictionConfig_shouldDelegate_maxSizePolicy_of_evictionConfig(MaxSizePolicy.USED_NATIVE_MEMORY_SIZE);
    }

    @Test
    public void cacheEvictionConfig_shouldDelegate_maxSizePolicy_of_evictionConfig_when_maxSizePolicyIs_usedNativeMemoryPercentage() {
        cacheEvictionConfig_shouldDelegate_maxSizePolicy_of_evictionConfig(MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);
    }

    @Test
    public void cacheEvictionConfig_shouldDelegate_maxSizePolicy_of_evictionConfig_when_maxSizePolicyIs_freeNativeMemorySize() {
        cacheEvictionConfig_shouldDelegate_maxSizePolicy_of_evictionConfig(MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE);
    }

    @Test
    public void cacheEvictionConfig_shouldDelegate_maxSizePolicy_of_evictionConfig_when_maxSizePolicyIs_freeNativeMemoryPercentage() {
        cacheEvictionConfig_shouldDelegate_maxSizePolicy_of_evictionConfig(MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE);
    }

    private void cacheEvictionConfig_shouldDelegate_maxSizePolicy_of_evictionConfig(MaxSizePolicy maxSizePolicy) {
        CacheMaxSizePolicy cacheMaxSizePolicy = fromMaxSizePolicy(maxSizePolicy);

        CacheEvictionConfig cacheEvictionConfig = new CacheEvictionConfig();
        cacheEvictionConfig.setMaxSizePolicy(cacheMaxSizePolicy);

        assertEquals(maxSizePolicy, cacheEvictionConfig.getMaxSizePolicy().toMaxSizePolicy());
    }
}
