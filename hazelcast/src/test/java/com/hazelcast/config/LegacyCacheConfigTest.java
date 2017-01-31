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

import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListener;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class LegacyCacheConfigTest extends HazelcastTestSupport {

    private SerializationService serializationService;
    private LegacyCacheConfig<Integer, Integer> config;

    @Before
    public void setUp() {
        serializationService = new DefaultSerializationServiceBuilder().build();

        CacheConfig<Integer, Integer> cacheConfig = new CacheConfig<Integer, Integer>();
        cacheConfig.addCacheEntryListenerConfiguration(new TestCacheEntryListenerConfiguration());

        config = new LegacyCacheConfig<Integer, Integer>(cacheConfig);
    }

    @Test
    public void testSerialization() {
        Data serializedConfig = serializationService.toData(config);
        LegacyCacheConfig deserializedConfig = serializationService.toObject(serializedConfig, LegacyCacheConfig.class);

        assertEquals(config.getClassType(), deserializedConfig.getClassType());
        assertEquals(config.getConfigAndReset(), deserializedConfig.getConfigAndReset());
    }

    private static class TestCacheEntryListenerConfiguration implements CacheEntryListenerConfiguration<Integer, Integer> {

        @Override
        public Factory<CacheEntryListener<? super Integer, ? super Integer>> getCacheEntryListenerFactory() {
            return null;
        }

        @Override
        public boolean isOldValueRequired() {
            return false;
        }

        @Override
        public Factory<CacheEntryEventFilter<? super Integer, ? super Integer>> getCacheEntryEventFilterFactory() {
            return null;
        }

        @Override
        public boolean isSynchronous() {
            return false;
        }

        @Override
        public boolean equals(Object obj) {
            return super.equals(obj) || (obj != null && obj instanceof TestCacheEntryListenerConfiguration);
        }

        @Override
        public int hashCode() {
            return 23;
        }
    }
}
