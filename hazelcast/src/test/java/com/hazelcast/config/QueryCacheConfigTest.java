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

package com.hazelcast.config;

import com.hazelcast.internal.config.QueryCacheConfigReadOnly;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryCacheConfigTest extends HazelcastTestSupport {

    @Test(expected = IllegalArgumentException.class)
    public void testSetName_throwsException_whenNameNull() {
        QueryCacheConfig config = new QueryCacheConfig();
        config.setName(null);
    }

    @Test(expected = NullPointerException.class)
    public void testSetPredicate_throwsException_whenPredicateNull() {
        QueryCacheConfig config = new QueryCacheConfig();
        config.setPredicateConfig(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetBatchSize_throwsException_whenNotPositive() {
        QueryCacheConfig config = new QueryCacheConfig();
        config.setBatchSize(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetBufferSize_throwsException_whenNotPositive() {
        QueryCacheConfig config = new QueryCacheConfig();
        config.setBufferSize(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetDelaySeconds_throwsException_whenNegative() {
        QueryCacheConfig config = new QueryCacheConfig();
        config.setDelaySeconds(-1);
    }

    @Test(expected = NullPointerException.class)
    public void testSetInMemoryFormat_throwsException_whenNull() {
        QueryCacheConfig config = new QueryCacheConfig();
        config.setInMemoryFormat(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetInMemoryFormat_throwsException_whenNative() {
        QueryCacheConfig config = new QueryCacheConfig();
        config.setInMemoryFormat(InMemoryFormat.NATIVE);
    }

    @Test(expected = NullPointerException.class)
    public void testSetEvictionConfig_throwsException_whenNull() {
        QueryCacheConfig config = new QueryCacheConfig();
        config.setEvictionConfig(null);
    }

    @Test(expected = NullPointerException.class)
    public void testAddEntryListenerConfig_throwsException_whenNull() {
        QueryCacheConfig config = new QueryCacheConfig();
        config.addEntryListenerConfig(null);
    }

    @Test(expected = NullPointerException.class)
    public void testSetEntryListenerConfigs_throwsException_whenNull() {
        QueryCacheConfig config = new QueryCacheConfig();
        config.setEntryListenerConfigs(null);
    }

    @Test
    public void testSetIndexConfigs_withNull() {
        QueryCacheConfig config = new QueryCacheConfig();
        config.setIndexConfigs(null);

        assertNotNull(config.getIndexConfigs());
        assertTrue(config.getIndexConfigs().isEmpty());
    }

    @Test
    public void testToString() {
        QueryCacheConfig config = new QueryCacheConfig();

        assertNotNull(config.toString());
        assertContains(config.toString(), "QueryCacheConfig");
    }


    @Test
    public void testEqualsAndHashCode() {
        assumeDifferentHashCodes();
        EqualsVerifier.forClass(QueryCacheConfig.class)
                .suppress(Warning.NONFINAL_FIELDS)
                .withPrefabValues(PredicateConfig.class,
                        new PredicateConfig("red"), new PredicateConfig("black"))
                .withPrefabValues(EvictionConfig.class,
                        new EvictionConfig().setSize(1000)
                                .setMaxSizePolicy(MaxSizePolicy.ENTRY_COUNT)
                                .setEvictionPolicy(EvictionPolicy.LFU),
                        new EvictionConfig().setSize(300)
                                .setMaxSizePolicy(MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE)
                                .setEvictionPolicy(EvictionPolicy.LRU))
                .withPrefabValues(QueryCacheConfigReadOnly.class,
                        new QueryCacheConfigReadOnly(new QueryCacheConfig("red")),
                        new QueryCacheConfigReadOnly(new QueryCacheConfig("black")))
                .verify();
    }
}
