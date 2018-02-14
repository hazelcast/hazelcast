/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl;

import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheConfigTest;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.helpers.CacheConfigHelper.getEvictionConfigByClassName;
import static com.hazelcast.config.helpers.CacheConfigHelper.getEvictionConfigByImplementation;
import static com.hazelcast.config.helpers.CacheConfigHelper.getEvictionConfigByPolicy;
import static com.hazelcast.config.helpers.CacheConfigHelper.newCompleteCacheConfig;
import static com.hazelcast.config.helpers.CacheConfigHelper.newDefaultCacheConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class PreJoinCacheConfigTest {

    private SerializationService serializationService;

    @Before
    public void setup() {
        serializationService = new DefaultSerializationServiceBuilder().build();
    }

    // Test transformation of CacheConfig to PreJoinCacheConfig and back to CacheConfig again
    @Test
    public void equalsCacheConfig_whenDefaultCacheConfig() {
        CacheConfig cacheConfig = newDefaultCacheConfig("test");
        PreJoinCacheConfig preJoinCacheConfig = new PreJoinCacheConfig(cacheConfig);
        assertEquals(cacheConfig, preJoinCacheConfig);
    }

    @Test
    public void equalsCacheConfig_whenCompleteCacheConfig() {
        CacheConfig cacheConfig = newCompleteCacheConfig("test");
        PreJoinCacheConfig preJoinCacheConfig = new PreJoinCacheConfig(cacheConfig);
        assertEquals(cacheConfig, preJoinCacheConfig);
    }

    @Test
    public void equalsCacheConfig_whenCacheConfig_withCacheLoaderAndWriter() {
        CacheConfig cacheConfig = newCompleteCacheConfig("test");
        cacheConfig.setCacheLoaderFactory(new CacheConfigTest.MyCacheLoaderFactory());
        cacheConfig.setCacheWriterFactory(new CacheConfigTest.MyCacheWriterFactory());
        PreJoinCacheConfig preJoinCacheConfig = new PreJoinCacheConfig(cacheConfig);
        assertEquals(cacheConfig, preJoinCacheConfig);
    }

    @Test
    public void equalsCacheConfig_whenCacheConfig_withEvictionConfigByPolicy() {
        CacheConfig cacheConfig = newCompleteCacheConfig("test");
        cacheConfig.setEvictionConfig(getEvictionConfigByPolicy());
        PreJoinCacheConfig preJoinCacheConfig = new PreJoinCacheConfig(cacheConfig);
        assertEquals(cacheConfig, preJoinCacheConfig);
    }

    @Test
    public void equalsCacheConfig_whenCacheConfig_withEvictionConfigByClassName() {
        CacheConfig cacheConfig = newCompleteCacheConfig("test");
        cacheConfig.setEvictionConfig(getEvictionConfigByClassName());
        PreJoinCacheConfig preJoinCacheConfig = new PreJoinCacheConfig(cacheConfig);
        assertEquals(cacheConfig, preJoinCacheConfig);
    }

    @Test
    public void equalsCacheConfig_whenCacheConfig_withEvictionConfigByImplementation() {
        CacheConfig cacheConfig = newCompleteCacheConfig("test");
        cacheConfig.setEvictionConfig(getEvictionConfigByImplementation());
        PreJoinCacheConfig preJoinCacheConfig = new PreJoinCacheConfig(cacheConfig);
        assertEquals(cacheConfig, preJoinCacheConfig);
    }

    // Test serialization & deserialization in the presence/absence of specified key/value types
    @Test
    public void serializationSucceeds_whenKVTypesNotSpecified() {
        CacheConfig cacheConfig = newDefaultCacheConfig("test");
        PreJoinCacheConfig preJoinCacheConfig = new PreJoinCacheConfig(cacheConfig);
        Data data = serializationService.toData(preJoinCacheConfig);
        PreJoinCacheConfig deserialized = serializationService.toObject(data);
        assertEquals(preJoinCacheConfig, deserialized);
        assertEquals(cacheConfig, deserialized.asCacheConfig());
    }

    @Test
    public void serializationSucceeds_whenKVTypes_setAsClassObjects() {
        CacheConfig cacheConfig = newDefaultCacheConfig("test");
        cacheConfig.setKeyType(Integer.class);
        cacheConfig.setValueType(String.class);
        PreJoinCacheConfig preJoinCacheConfig = new PreJoinCacheConfig(cacheConfig);
        Data data = serializationService.toData(preJoinCacheConfig);
        PreJoinCacheConfig deserialized = serializationService.toObject(data);
        assertEquals(preJoinCacheConfig, deserialized);
        assertEquals(cacheConfig, deserialized.asCacheConfig());
    }

    @Test
    public void serializationSucceeds_whenKVTypes_setAsClassNames() {
        CacheConfig cacheConfig = newDefaultCacheConfig("test");
        cacheConfig.setKeyClassName("java.lang.Integer");
        cacheConfig.setValueClassName("java.lang.String");
        PreJoinCacheConfig preJoinCacheConfig = new PreJoinCacheConfig(cacheConfig);
        Data data = serializationService.toData(preJoinCacheConfig);
        PreJoinCacheConfig deserialized = serializationService.toObject(data);
        assertEquals(preJoinCacheConfig, deserialized);
        assertEquals(cacheConfig, deserialized.asCacheConfig());
    }

    @Test
    public void serializationSucceeds_whenKeyTypeNotResolvable() {
        PreJoinCacheConfig preJoinCacheConfig = new PreJoinCacheConfig(newDefaultCacheConfig("test"));
        preJoinCacheConfig.setKeyClassName("some.inexistent.Class");
        preJoinCacheConfig.setValueClassName("java.lang.String");
        Data data = serializationService.toData(preJoinCacheConfig);
        PreJoinCacheConfig deserialized = serializationService.toObject(data);
        assertEquals(deserialized, preJoinCacheConfig);
        try {
            Class klass = deserialized.asCacheConfig().getKeyType();
            fail("Getting the key type on deserialized CacheConfig should fail because the key type cannot be resolved");
        } catch (HazelcastException e) {
            if (!(e.getCause() instanceof ClassNotFoundException)) {
                fail("Unexpected exception: " + e.getCause());
            }
        }
    }

    @Test
    public void serializationSucceeds_whenValueTypeNotResolvable() {
        PreJoinCacheConfig preJoinCacheConfig = new PreJoinCacheConfig(newDefaultCacheConfig("test"));
        preJoinCacheConfig.setKeyClassName("java.lang.String");
        preJoinCacheConfig.setValueClassName("some.inexistent.Class");
        Data data = serializationService.toData(preJoinCacheConfig);
        PreJoinCacheConfig deserialized = serializationService.toObject(data);
        assertEquals(deserialized, preJoinCacheConfig);
        try {
            Class klass = deserialized.asCacheConfig().getValueType();
            fail("Getting the value type on deserialized CacheConfig should fail because the value type cannot be resolved");
        } catch (HazelcastException e) {
            if (!(e.getCause() instanceof ClassNotFoundException)) {
                fail("Unexpected exception: " + e.getCause());
            }
        }
    }

}
