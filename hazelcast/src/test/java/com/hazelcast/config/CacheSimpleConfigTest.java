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

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.ENTRY_COUNT;
import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CacheSimpleConfigTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

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
    public void testEqualsAndHashCode() {
        CacheSimpleEntryListenerConfig redEntryListenerConfig = new CacheSimpleEntryListenerConfig();
        redEntryListenerConfig.setCacheEntryListenerFactory("red");
        CacheSimpleEntryListenerConfig blackEntryListenerConfig = new CacheSimpleEntryListenerConfig();
        blackEntryListenerConfig.setCacheEntryListenerFactory("black");
        EqualsVerifier.forClass(CacheSimpleConfig.class)
                      .allFieldsShouldBeUsedExcept("readOnly")
                      .suppress(Warning.NONFINAL_FIELDS, Warning.NULL_FIELDS)
                      .withPrefabValues(EvictionConfig.class,
                              new EvictionConfig(1000, ENTRY_COUNT, EvictionPolicy.LFU),
                              new EvictionConfig(300, USED_NATIVE_MEMORY_PERCENTAGE, EvictionPolicy.LRU))
                      .withPrefabValues(WanReplicationRef.class,
                              new WanReplicationRef("red", null, null,false),
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
                      .verify();
    }
}
