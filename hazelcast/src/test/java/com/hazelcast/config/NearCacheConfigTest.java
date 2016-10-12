/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.eviction.EvictionPolicyType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.ENTRY_COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class NearCacheConfigTest {

    private NearCacheConfig config = new NearCacheConfig();

    /**
     * Test method for {@link com.hazelcast.config.NearCacheConfigReadOnly#setCacheLocalEntries(boolean)} .
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testReadOnlyNearCacheConfigSetCacheLocalEntries() {
        new NearCacheConfigReadOnly(new NearCacheConfig()).setCacheLocalEntries(true);
    }

    @Test
    public void testMaxSize_whenValueIsZero_thenSetIntegerMax() {
        config.setMaxSize(0);

        assertEquals(Integer.MAX_VALUE, config.getMaxSize());
    }

    @Test
    public void testMaxSize_whenValueIsPositive_thenSetValue() {
        config.setMaxSize(4531);

        assertEquals(4531, config.getMaxSize());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMaxSize_whenValueIsNegative_thenThrowException() {
        config.setMaxSize(-1);
    }

    @Test(expected = NullPointerException.class)
    public void testSetEvictionConfig_whenNull_thenThrowException() {
        config.setEvictionConfig(null);
    }

    @Test
    public void testEvictionConversion_whenMaxSizeAndEvictionPolicyIsSet_thenEvictionIsConfigured() {
        config.setMaxSize(123);
        config.setEvictionPolicy("LFU");

        EvictionConfig evictionConfig = config.getEvictionConfig();
        assertEquals(123, evictionConfig.getSize());
        assertEquals(EvictionPolicyType.LFU, evictionConfig.getEvictionPolicyType());
        assertEquals(ENTRY_COUNT, evictionConfig.getMaximumSizePolicy());
    }

    @Test
    public void testEvictionConversion_whenEvictionIsSet_thenMaxSizeAndEvictionPolicyIsNotConfigured() {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setSize(4453)
                .setEvictionPolicy(EvictionPolicy.LFU)
                .setMaximumSizePolicy(ENTRY_COUNT);

        config.setEvictionConfig(evictionConfig);

        assertNotEquals(4453, config.getMaxSize());
        assertNotEquals("LFU", config.getEvictionPolicy());
    }

    @Test
    public void testEvictionConversion_whenExistingEvictionIsModified_thenMaxSizeAndEvictionPolicyIsNotConfigured() {
        config.getEvictionConfig()
                .setSize(15125)
                .setEvictionPolicy(EvictionPolicy.LFU)
                .setMaximumSizePolicy(ENTRY_COUNT);

        assertNotEquals(15125, config.getMaxSize());
        assertNotEquals("LFU", config.getEvictionPolicy());
    }
}
