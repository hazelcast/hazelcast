/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.ENTRY_COUNT;
import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class NearCacheConfigTest {

    private NearCacheConfig config = new NearCacheConfig();

    @Test
    public void testConstructor_withName() {
        config = new NearCacheConfig("foobar");

        assertEquals("foobar", config.getName());
    }

    @Test
    public void testConstructor_withMultipleParameters() {
        config = new NearCacheConfig(23, 42, true, InMemoryFormat.NATIVE);

        assertEquals(23, config.getTimeToLiveSeconds());
        assertEquals(42, config.getMaxIdleSeconds());
        assertTrue(config.isInvalidateOnChange());
        assertEquals(InMemoryFormat.NATIVE, config.getInMemoryFormat());
    }

    @Test
    public void testConstructor_withMultipleParametersAndEvictionConfig() {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setEvictionPolicy(EvictionPolicy.LFU)
                .setMaximumSizePolicy(USED_NATIVE_MEMORY_PERCENTAGE)
                .setSize(66);

        config = new NearCacheConfig(23, 42, true, InMemoryFormat.NATIVE, evictionConfig);

        assertEquals(23, config.getTimeToLiveSeconds());
        assertEquals(42, config.getMaxIdleSeconds());
        assertTrue(config.isInvalidateOnChange());
        assertEquals(InMemoryFormat.NATIVE, config.getInMemoryFormat());
        assertEquals(EvictionPolicy.LFU, config.getEvictionConfig().getEvictionPolicy());
        assertEquals(USED_NATIVE_MEMORY_PERCENTAGE, config.getEvictionConfig().getMaximumSizePolicy());
        assertEquals(66, config.getEvictionConfig().getSize());
    }

    @Test
    public void testDeprecatedConstructor_withMultipleParameters() {
        config = new NearCacheConfig(23, 5000, EvictionPolicy.RANDOM.name(), 42, true, InMemoryFormat.NATIVE);

        assertEquals(23, config.getTimeToLiveSeconds());
        assertEquals(5000, config.getMaxSize());
        assertEquals(EvictionPolicy.RANDOM.name(), config.getEvictionPolicy());
        assertEquals(42, config.getMaxIdleSeconds());
        assertTrue(config.isInvalidateOnChange());
        assertEquals(InMemoryFormat.NATIVE, config.getInMemoryFormat());
    }

    @Test
    public void testDeprecatedConstructor_withMultipleParametersAndEvictionConfig() {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setEvictionPolicy(EvictionPolicy.LFU)
                .setMaximumSizePolicy(USED_NATIVE_MEMORY_PERCENTAGE)
                .setSize(66);

        config = new NearCacheConfig(23, 5000, EvictionPolicy.RANDOM.name(), 42, true, InMemoryFormat.NATIVE, evictionConfig);

        assertEquals(23, config.getTimeToLiveSeconds());
        assertEquals(5000, config.getMaxSize());
        assertEquals(EvictionPolicy.RANDOM.name(), config.getEvictionPolicy());
        assertEquals(42, config.getMaxIdleSeconds());
        assertTrue(config.isInvalidateOnChange());
        assertEquals(InMemoryFormat.NATIVE, config.getInMemoryFormat());
        assertEquals(EvictionPolicy.LFU, config.getEvictionConfig().getEvictionPolicy());
        assertEquals(USED_NATIVE_MEMORY_PERCENTAGE, config.getEvictionConfig().getMaximumSizePolicy());
        assertEquals(66, config.getEvictionConfig().getSize());
    }

    @Test
    public void testSetInMemoryFormat_withString() {
        config.setInMemoryFormat("NATIVE");

        assertEquals(InMemoryFormat.NATIVE, config.getInMemoryFormat());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetInMemoryFormat_withInvalidString() {
        config.setInMemoryFormat("UNKNOWN");
    }

    @Test(expected = NullPointerException.class)
    public void testSetInMemoryFormat_withString_whenNull() {
        config.setInMemoryFormat((String) null);
    }

    @Test
    public void testIsSerializeKeys_whenEnabled() {
        config.setSerializeKeys(true);
        assertTrue(config.isSerializeKeys());
    }

    @Test
    public void testIsSerializeKeys_whenDisabled() {
        config.setSerializeKeys(false);
        assertFalse(config.isSerializeKeys());
    }

    @Test
    public void testIsSerializeKeys_whenNativeMemoryFormat_thenAlwaysReturnTrue() {
        config.setSerializeKeys(false);
        config.setInMemoryFormat(InMemoryFormat.NATIVE);
        assertTrue(config.isSerializeKeys());
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
        assertEquals(EvictionPolicy.LFU, evictionConfig.getEvictionPolicy());
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

    @Test
    public void testSetNearCachePreloaderConfig() {
        NearCachePreloaderConfig preloaderConfig = new NearCachePreloaderConfig();

        config.setPreloaderConfig(preloaderConfig);

        assertEquals(preloaderConfig, config.getPreloaderConfig());
    }

    @Test(expected = NullPointerException.class)
    public void testSetNearCachePreloaderConfig_whenNull_thenThrowException() {
        config.setPreloaderConfig(null);
    }

    @Test
    public void testSerialization() {
        config.setInvalidateOnChange(true);
        config.setCacheLocalEntries(true);
        config.setName("foobar");
        config.setInMemoryFormat(InMemoryFormat.NATIVE);
        config.setTimeToLiveSeconds(23);
        config.setMaxIdleSeconds(42);
        config.setLocalUpdatePolicy(NearCacheConfig.LocalUpdatePolicy.CACHE_ON_UPDATE);

        SerializationService serializationService = new DefaultSerializationServiceBuilder().build();
        Data serialized = serializationService.toData(config);
        NearCacheConfig deserialized = serializationService.toObject(serialized);

        assertEquals(config.isInvalidateOnChange(), deserialized.isInvalidateOnChange());
        assertEquals(config.isCacheLocalEntries(), deserialized.isCacheLocalEntries());
        assertEquals(config.getName(), deserialized.getName());
        assertEquals(config.getInMemoryFormat(), deserialized.getInMemoryFormat());
        assertEquals(config.getTimeToLiveSeconds(), deserialized.getTimeToLiveSeconds());
        assertEquals(config.getMaxIdleSeconds(), deserialized.getMaxIdleSeconds());
        assertEquals(config.getLocalUpdatePolicy(), deserialized.getLocalUpdatePolicy());
        assertEquals(config.getEvictionPolicy(), deserialized.getEvictionPolicy());
        assertEquals(config.getMaxSize(), deserialized.getMaxSize());
        assertEquals(config.toString(), deserialized.toString());
    }
}
