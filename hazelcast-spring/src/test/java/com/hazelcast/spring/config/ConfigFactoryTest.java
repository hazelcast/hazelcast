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

package com.hazelcast.spring.config;

import com.hazelcast.config.CompactSerializationConfig;
import com.hazelcast.config.CompactSerializationConfigAccessor;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.internal.util.TriTuple;
import com.hazelcast.spi.eviction.EvictionPolicyComparator;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class ConfigFactoryTest {

    @SuppressWarnings("rawtypes")
    private static final EvictionPolicyComparator dummyComparator = (o1, o2) -> 0;
    private static final String SOME_COMPARATOR_CLASS_NAME = dummyComparator.getClass().getName();

    @Test
    public void should_create_eviction_config() {

        EvictionConfig evictionConfig = ConfigFactory.newEvictionConfig(42, MaxSizePolicy.PER_NODE, EvictionPolicy.LRU, false,
                false, null, dummyComparator);
        assertThat(evictionConfig.getSize()).isEqualTo(42);
        assertThat(evictionConfig.getMaxSizePolicy()).isEqualTo(MaxSizePolicy.PER_NODE);
        assertThat(evictionConfig.getEvictionPolicy()).isEqualTo(EvictionPolicy.LRU);
        assertThat(evictionConfig.getComparator()).isSameAs(dummyComparator);
    }

    @Test
    public void should_create_eviction_config_with_comparator_class_name() {

        EvictionConfig evictionConfig = ConfigFactory.newEvictionConfig(42, MaxSizePolicy.PER_NODE, EvictionPolicy.LRU, false,
                false, SOME_COMPARATOR_CLASS_NAME, null);
        assertThat(evictionConfig.getComparatorClassName()).isEqualTo(SOME_COMPARATOR_CLASS_NAME);
    }

    @Test
    public void should_use_default_map_max_size_for_null_size() {
        EvictionConfig evictionConfig = newEvictionConfig(null, true);
        assertThat(evictionConfig.getSize()).isEqualTo(MapConfig.DEFAULT_MAX_SIZE);
    }

    @Test
    public void should_use_default_map_max_size_for_0_size() {
        EvictionConfig evictionConfig = newEvictionConfig(0, true);
        assertThat(evictionConfig.getSize()).isEqualTo(MapConfig.DEFAULT_MAX_SIZE);
    }

    @Test
    public void should_use_default_cache_max_size_for_null_size() {
        EvictionConfig evictionConfig = newEvictionConfig(null, false);
        assertThat(evictionConfig.getSize()).isEqualTo(EvictionConfig.DEFAULT_MAX_ENTRY_COUNT);
    }

    @Test
    public void should_use_provided_eviction_policy() {
        EvictionConfig evictionConfig = newEvictionConfigWithEvictionPolicy(EvictionPolicy.LRU, false);
        assertThat(evictionConfig.getEvictionPolicy()).isEqualTo(EvictionPolicy.LRU);
    }

    @Test
    public void should_use_default_map_eviction_policy() {
        EvictionConfig evictionConfig = newEvictionConfigWithEvictionPolicy(null, true);
        assertThat(evictionConfig.getEvictionPolicy()).isEqualTo(MapConfig.DEFAULT_EVICTION_POLICY);
    }

    @Test
    public void should_use_default_cache_eviction_policy() {
        EvictionConfig evictionConfig = newEvictionConfigWithEvictionPolicy(null, false);
        assertThat(evictionConfig.getEvictionPolicy()).isEqualTo(EvictionConfig.DEFAULT_EVICTION_POLICY);
    }

    @Test
    public void should_use_provided_max_size_policy() {
        EvictionConfig evictionConfig = newEvictionConfigWithMaxSizePolicy(MaxSizePolicy.PER_NODE, false);
        assertThat(evictionConfig.getMaxSizePolicy()).isEqualTo(MaxSizePolicy.PER_NODE);
    }

    @Test
    public void should_use_default_cache_max_size_policy() {
        EvictionConfig evictionConfig = newEvictionConfigWithMaxSizePolicy(null, false);
        assertThat(evictionConfig.getMaxSizePolicy()).isEqualTo(EvictionConfig.DEFAULT_MAX_SIZE_POLICY);
    }

    @Test
    public void should_use_default_map_max_size_policy() {
        EvictionConfig evictionConfig = newEvictionConfigWithMaxSizePolicy(null, true);
        assertThat(evictionConfig.getMaxSizePolicy()).isEqualTo(MapConfig.DEFAULT_MAX_SIZE_POLICY);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void should_perform_checks_for_common_eviction_config() {
        invalidEvictionConfig(false, false);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void should_perform_checks_for_cache_eviction_config() {
        invalidEvictionConfig(true, true);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void should_perform_checks_for_map_eviction_config() {
        invalidEvictionConfig(false, true);
    }

    @Test
    public void should_create_compact_serialization_config() {
        Map<String, TriTuple<String, String, String>> registrations = new HashMap<>();
        TriTuple<String, String, String> registration = TriTuple.of("a", "b", "c");
        registrations.put("b", registration);
        CompactSerializationConfig config = ConfigFactory.newCompactSerializationConfig(true, registrations);
        assertThat(config.isEnabled()).isTrue();
        assertThat(CompactSerializationConfigAccessor.getNamedRegistrations(config)).isEqualTo(registrations);
    }

    @Test
    public void should_create_compact_serialization_config_with_reflective_serializer() {
        Map<String, TriTuple<String, String, String>> registrations = new HashMap<>();
        TriTuple<String, String, String> registration = TriTuple.of("a", "a", null);
        registrations.put("a", registration);
        CompactSerializationConfig config = ConfigFactory.newCompactSerializationConfig(true, registrations);
        assertThat(config.isEnabled()).isTrue();
        assertThat(CompactSerializationConfigAccessor.getNamedRegistrations(config)).isEqualTo(registrations);
    }

    private static void invalidEvictionConfig(boolean isNearCache, boolean isIMap) {
        ConfigFactory.newEvictionConfig(null, null, null, isNearCache,
                isIMap, SOME_COMPARATOR_CLASS_NAME, dummyComparator);
    }

    private static EvictionConfig newEvictionConfigWithMaxSizePolicy(MaxSizePolicy maxSizePolicy, boolean isIMap) {
        return ConfigFactory.newEvictionConfig(null, maxSizePolicy, null, false,
                isIMap, null, null);
    }

    private static EvictionConfig newEvictionConfigWithEvictionPolicy(EvictionPolicy evictionPolicy, boolean isIMap) {
        return ConfigFactory.newEvictionConfig(null, MaxSizePolicy.PER_NODE, evictionPolicy, false,
                isIMap, null, null);
    }

    private static EvictionConfig newEvictionConfig(Integer maxSize, boolean isIMap) {
        return ConfigFactory.newEvictionConfig(maxSize, MaxSizePolicy.PER_NODE, EvictionPolicy.LRU, false,
                isIMap, null, null);
    }

}
