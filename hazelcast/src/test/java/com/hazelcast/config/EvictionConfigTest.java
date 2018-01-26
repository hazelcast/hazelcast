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

import com.hazelcast.internal.eviction.EvictionPolicyComparator;
import com.hazelcast.internal.eviction.impl.comparator.LFUEvictionPolicyComparator;
import com.hazelcast.internal.eviction.impl.comparator.LRUEvictionPolicyComparator;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.ENTRY_COUNT;
import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class EvictionConfigTest {

    @Test
    public void testEqualsAndHashCode() {
        EqualsVerifier.forClass(EvictionConfig.class)
                      .allFieldsShouldBeUsedExcept("readOnly", "sizeConfigured")
                      .suppress(Warning.NONFINAL_FIELDS)
                      .withPrefabValues(EvictionConfig.class,
                              new EvictionConfig(1000, ENTRY_COUNT, EvictionPolicy.LFU),
                              new EvictionConfig(300, USED_NATIVE_MEMORY_PERCENTAGE, EvictionPolicy.LRU))
                      .withPrefabValues(EvictionPolicyComparator.class,
                              new LFUEvictionPolicyComparator(), new LRUEvictionPolicyComparator())
                      .verify();
    }

}
