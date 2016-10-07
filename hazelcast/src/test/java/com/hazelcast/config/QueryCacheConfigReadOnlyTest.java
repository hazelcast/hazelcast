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

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class QueryCacheConfigReadOnlyTest {

    private QueryCacheConfigReadOnly getMapStoreConfigReadOnly() {
        return new QueryCacheConfig().getAsReadOnly();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void gettingIndexConfigsOfReadOnlyQueryCacheConfigShouldReturnUnmodifiable() {
        QueryCacheConfig config = new QueryCacheConfig()
                .addIndexConfig(new MapIndexConfig())
                .addIndexConfig(new MapIndexConfig());

        List<MapIndexConfig> indexConfigs = config.getAsReadOnly().getIndexConfigs();
        indexConfigs.add(new MapIndexConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void gettingEntryListenerConfigsOfReadOnlyQueryCacheConfigShouldReturnUnmodifiable() {
        QueryCacheConfig config = new QueryCacheConfig()
                .addEntryListenerConfig(new EntryListenerConfig())
                .addEntryListenerConfig(new EntryListenerConfig());

        List<EntryListenerConfig> entryListenerConfigs = config.getAsReadOnly().getEntryListenerConfigs();
        entryListenerConfigs.add(new EntryListenerConfig());
    }

    @Test
    public void gettingEvictionConfigShouldReturnReadOnlyConfig() {
        assertTrue(getMapStoreConfigReadOnly().getEvictionConfig() instanceof EvictionConfigReadOnly);
    }

    @Test
    public void gettingPredicateConfigShouldReturnReadOnlyConfig() {
        assertTrue(getMapStoreConfigReadOnly().getPredicateConfig() instanceof PredicateConfigReadOnly);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingBatchSizeOfReadOnlyMapStoreConfigShouldFail() {
        getMapStoreConfigReadOnly().setBatchSize(5);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingBufferSizeOfReadOnlyMapStoreConfigShouldFail() {
        getMapStoreConfigReadOnly().setBufferSize(3);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingDelaySecondsOfReadOnlyMapStoreConfigShouldFail() {
        getMapStoreConfigReadOnly().setDelaySeconds(3);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingEntryListenerConfigsOfReadOnlyMapStoreConfigShouldFail() {
        getMapStoreConfigReadOnly().setEntryListenerConfigs(singletonList(new EntryListenerConfig()));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingEvictionConfigOfReadOnlyMapStoreConfigShouldFail() {
        getMapStoreConfigReadOnly().setEvictionConfig(new EvictionConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingIncludeValueOfReadOnlyMapStoreConfigShouldFail() {
        getMapStoreConfigReadOnly().setIncludeValue(true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingIndexConfigsOfReadOnlyMapStoreConfigShouldFail() {
        getMapStoreConfigReadOnly().setIndexConfigs(singletonList(new MapIndexConfig()));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingInMemoryFormatOfReadOnlyMapStoreConfigShouldFail() {
        getMapStoreConfigReadOnly().setInMemoryFormat(InMemoryFormat.OBJECT);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingNameOfReadOnlyMapStoreConfigShouldFail() {
        getMapStoreConfigReadOnly().setName("myQueryCache");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingPredicateConfigOfReadOnlyMapStoreConfigShouldFail() {
        getMapStoreConfigReadOnly().setPredicateConfig(new PredicateConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingPopulateOfReadOnlyMapStoreConfigShouldFail() {
        getMapStoreConfigReadOnly().setPopulate(true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingCoalesceOfReadOnlyMapStoreConfigShouldFail() {
        getMapStoreConfigReadOnly().setCoalesce(true);
    }
}
