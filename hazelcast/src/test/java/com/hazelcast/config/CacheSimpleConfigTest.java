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

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CacheSimpleConfigTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testNameInConstructor() {
        String name = randomMapName();
        CacheSimpleConfig config = new CacheSimpleConfig(name);

        assertEquals(name, config.getName());
    }

    @Test
    public void givenCacheLoaderIsConfigured_whenConfigureCacheLoaderFactory_thenThrowIllegalStateException() {
        CacheSimpleConfig config = new CacheSimpleConfig();
        config.setCacheLoader("foo");

        expectedException.expect(IllegalStateException.class);
        config.setCacheLoaderFactory("bar");
    }

    @Test
    public void givenCacheLoaderFactoryIsConfigured_whenConfigureCacheLoader_thenThrowIllegalStateException() {
        CacheSimpleConfig config = new CacheSimpleConfig();
        config.setCacheLoaderFactory("bar");

        expectedException.expect(IllegalStateException.class);
        config.setCacheLoader("foo");
    }

    @Test
    public void givenCacheWriterIsConfigured_whenConfigureCacheWriterFactory_thenThrowIllegalStateException() {
        CacheSimpleConfig config = new CacheSimpleConfig();
        config.setCacheWriter("foo");

        expectedException.expect(IllegalStateException.class);
        config.setCacheWriterFactory("bar");
    }

    @Test
    public void givenCacheWriterFactoryIsConfigured_whenConfigureCacheWriter_thenThrowIllegalStateException() {
        CacheSimpleConfig config = new CacheSimpleConfig();
        config.setCacheWriterFactory("bar");

        expectedException.expect(IllegalStateException.class);
        config.setCacheWriter("foo");
    }

    @Test
    public void givenNullMerkleTreeConfig_throws_NPE() {
        expectedException.expect(NullPointerException.class);

        new CacheSimpleConfig()
                .setMerkleTreeConfig(null);
    }

    @Test
    public void testEqualsAndHashCode() {
        assumeDifferentHashCodes();

        CacheSimpleEntryListenerConfig redEntryListenerConfig = new CacheSimpleEntryListenerConfig();
        redEntryListenerConfig.setCacheEntryListenerFactory("red");

        CacheSimpleEntryListenerConfig blackEntryListenerConfig = new CacheSimpleEntryListenerConfig();
        blackEntryListenerConfig.setCacheEntryListenerFactory("black");

        EqualsVerifier.forClass(CacheSimpleConfig.class)
                .suppress(Warning.NONFINAL_FIELDS, Warning.NULL_FIELDS)
                .withPrefabValues(EvictionConfig.class,
                        new EvictionConfig().setSize(1000)
                                .setMaxSizePolicy(MaxSizePolicy.ENTRY_COUNT)
                                .setEvictionPolicy(EvictionPolicy.LFU),
                        new EvictionConfig().setSize(300)
                                .setMaxSizePolicy(MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE)
                                .setEvictionPolicy(EvictionPolicy.LRU))
                .withPrefabValues(WanReplicationRef.class,
                        new WanReplicationRef("red", null, null, false),
                        new WanReplicationRef("black", null, null, true))
                .withPrefabValues(CacheSimpleConfig.class,
                        new CacheSimpleConfig().setName("red"),
                        new CacheSimpleConfig().setName("black"))
                .withPrefabValues(CacheSimpleEntryListenerConfig.class,
                        redEntryListenerConfig,
                        blackEntryListenerConfig)
                .withPrefabValues(CachePartitionLostListenerConfig.class,
                        new CachePartitionLostListenerConfig("red"),
                        new CachePartitionLostListenerConfig("black"))
                .withPrefabValues(MergePolicyConfig.class,
                        new MergePolicyConfig("policy1", 1),
                        new MergePolicyConfig("policy2", 2))
                .verify();
    }
}
