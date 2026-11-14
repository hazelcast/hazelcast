/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.spi.eviction.EvictionPolicyComparator;
import com.hazelcast.internal.eviction.impl.comparator.LFUEvictionPolicyComparator;
import com.hazelcast.internal.eviction.impl.comparator.LRUEvictionPolicyComparator;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.MaxSizePolicy.ENTRY_COUNT;
import static com.hazelcast.config.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE;
import static com.hazelcast.config.EvictionPolicy.LFU;
import static com.hazelcast.config.EvictionPolicy.LRU;
import static com.hazelcast.test.HazelcastTestSupport.assumeDifferentHashCodes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EvictionConfigTest {

    @Test
    public void testEqualsAndHashCode() {
        assumeDifferentHashCodes();
        EqualsVerifier.forClass(EvictionConfig.class)
                .withIgnoredFields("sizeConfigured")
                .suppress(Warning.NONFINAL_FIELDS)
                .withPrefabValues(EvictionConfig.class,
                        new EvictionConfig().setSize(1000).setMaxSizePolicy(ENTRY_COUNT).setEvictionPolicy(LFU),
                        new EvictionConfig().setSize(300).setMaxSizePolicy(USED_NATIVE_MEMORY_PERCENTAGE).setEvictionPolicy(LRU))
                .withPrefabValues(EvictionPolicyComparator.class,
                        new LFUEvictionPolicyComparator(), new LRUEvictionPolicyComparator())
                .verify();
    }

    @Test
    public void testSerialization_preservesConfiguredSize() {
        InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();
        EvictionConfig config = new EvictionConfig().setSize(1000);

        EvictionConfig deserialized = serializationService.toObject(serializationService.toData(config));

        assertEquals(1000, deserialized.getSize());
        assertTrue(deserialized.sizeConfigured);
        NearCacheConfig nearCacheConfig = new NearCacheConfig().setEvictionConfig(deserialized);
        assertSame(nearCacheConfig,
                NearCacheConfigAccessor.copyWithInitializedDefaultMaxSizeForOnHeapMaps(nearCacheConfig));
    }

    @Test
    public void testSerialization_unconfiguredSize_remainsUnconfigured() {
        InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();

        EvictionConfig deserialized = serializationService.toObject(
                serializationService.toData(new EvictionConfig()));

        assertEquals(EvictionConfig.DEFAULT_MAX_ENTRY_COUNT, deserialized.getSize());
        assertFalse(deserialized.sizeConfigured);
    }

}
