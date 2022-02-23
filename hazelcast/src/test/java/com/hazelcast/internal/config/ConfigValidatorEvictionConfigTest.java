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

package com.hazelcast.internal.config;

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.config.ConfigValidator.COMMONLY_SUPPORTED_EVICTION_POLICIES;
import static com.hazelcast.internal.config.ConfigValidator.checkCacheEvictionConfig;
import static com.hazelcast.internal.config.ConfigValidator.checkCacheMaxSizePolicy;
import static com.hazelcast.internal.config.ConfigValidator.checkMapMaxSizePolicyPerInMemoryFormat;
import static com.hazelcast.internal.config.ConfigValidator.checkNearCacheEvictionConfig;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ConfigValidatorEvictionConfigTest extends HazelcastTestSupport {

    @Test
    public void checkEvictionConfig_forCache() {
        checkCacheEvictionConfig(getEvictionConfig(false, false));
    }

    @Test(expected = IllegalArgumentException.class)
    public void checkEvictionConfig_forCache_when_wrong_max_size_policy() {
        EvictionConfig evictionConfig = getEvictionConfig(false, false);
        evictionConfig.setMaxSizePolicy(MaxSizePolicy.PER_PARTITION);

        checkCacheMaxSizePolicy(evictionConfig.getMaxSizePolicy(), InMemoryFormat.NATIVE);
    }

    @Test
    public void checkEvictionConfig_forMap() {
        EvictionConfig evictionConfig = getEvictionConfig(false, false);
        evictionConfig.setMaxSizePolicy(MaxSizePolicy.PER_PARTITION);

        ConfigValidator.checkMapEvictionConfig(evictionConfig);
    }

    @Test
    public void checkEvictionConfig_forNearCache() {
        EvictionConfig evictionConfig = getEvictionConfig(false, false, EvictionPolicy.RANDOM);
        checkNearCacheEvictionConfig(evictionConfig.getEvictionPolicy(), null, null);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void checkEvictionConfig_withNull() {
        checkEvictionConfig(null);
    }

    @Test
    public void checkEvictionConfig_whenBothOfComparatorAndComparatorClassNameAreSet_oneOnlyShouldBePresent() {
        checkEvictionConfig(getEvictionConfig(true, true));
    }

    @Test
    public void checkEvictionConfig_whenBothOfComparatorAndComparatorClassNameAreSet_forNearCache_oneOnlyShouldBePresent() {
        EvictionConfig evictionConfig = getEvictionConfig(true, true);
        checkNearCacheEvictionConfig(evictionConfig.getEvictionPolicy(),
                evictionConfig.getComparatorClassName(), evictionConfig.getComparator());
    }

    @Test(expected = InvalidConfigurationException.class)
    public void checkEvictionConfig_whenNoneOfTheComparatorAndComparatorClassNameAreSetIfEvictionPolicyIsNone() {
        EvictionConfig evictionConfig = getEvictionConfig(false, false, EvictionPolicy.NONE);
        checkEvictionConfig(evictionConfig);
    }

    @Test
    public void checkEvictionConfig_whenComparatorClassNameIsSetIfEvictionPolicyIsNone() {
        checkEvictionConfig(getEvictionConfig(true, false, EvictionPolicy.NONE));
    }

    @Test
    public void checkEvictionConfig_whenComparatorIsSetIfEvictionPolicyIsNone() {
        checkEvictionConfig(getEvictionConfig(false, true, EvictionPolicy.NONE));
    }

    @Test(expected = InvalidConfigurationException.class)
    public void checkEvictionConfig_whenNoneOfTheComparatorAndComparatorClassNameAreSetIfEvictionPolicyIsRandom() {
        checkEvictionConfig(getEvictionConfig(false, false, EvictionPolicy.RANDOM));
    }

    @Test
    public void checkEvictionConfig_whenComparatorClassNameIsSetIfEvictionPolicyIsRandom() {
        checkEvictionConfig(getEvictionConfig(true, false, EvictionPolicy.RANDOM));
    }

    @Test
    public void checkEvictionConfig_whenComparatorIsSetIfEvictionPolicyIsRandom() {
        checkEvictionConfig(getEvictionConfig(false, true, EvictionPolicy.RANDOM));
    }

    @Test
    public void checkEvictionConfig_whenComparatorClassNameIsSetIfEvictionPolicyIsNotSet() {
        checkEvictionConfig(getEvictionConfig(true, false));
    }

    @Test
    public void checkEvictionConfig_whenComparatorIsSetIfEvictionPolicyIsNotSet() {
        checkEvictionConfig(getEvictionConfig(false, true));
    }

    @Test(expected = InvalidConfigurationException.class)
    public void checkEvictionConfig_whenComparatorClassNameIsSetIfEvictionPolicyIsAlsoSet() {
        // default eviction policy is LRU (see EvictionConfig.DEFAULT_EVICTION_POLICY)
        checkEvictionConfig(getEvictionConfig(true, false, EvictionPolicy.LFU));
    }

    @Test(expected = InvalidConfigurationException.class)
    public void checkEvictionConfig_whenComparatorIsSetIfEvictionPolicyIsAlsoSet() {
        // default eviction policy is LRU (see EvictionConfig.DEFAULT_EVICTION_POLICY)
        checkEvictionConfig(getEvictionConfig(false, true, EvictionPolicy.LFU));
    }

    @Test
    public void checkEvictionConfig_whenNoneOfTheComparatorAndComparatorClassNameAreSetIfEvictionPolicyIsNone_forNearCache() {
        EvictionConfig evictionConfig = getEvictionConfig(false, false, EvictionPolicy.NONE);
        checkNearCacheEvictionConfig(evictionConfig.getEvictionPolicy(),
                evictionConfig.getComparatorClassName(), evictionConfig.getComparator());
    }

    @Test(expected = InvalidConfigurationException.class)
    public void checkEvictionConfig_whenComparatorClassNameIsSetIfEvictionPolicyIsNone_forNearCache() {
        EvictionConfig evictionConfig = getEvictionConfig(true, false, EvictionPolicy.NONE);
        checkNearCacheEvictionConfig(evictionConfig.getEvictionPolicy(),
                evictionConfig.getComparatorClassName(), evictionConfig.getComparator());
    }

    @Test(expected = InvalidConfigurationException.class)
    public void checkEvictionConfig_whenComparatorIsSetIfEvictionPolicyIsNone_forNearCache() {
        EvictionConfig evictionConfig = getEvictionConfig(false, true, EvictionPolicy.NONE);
        checkNearCacheEvictionConfig(evictionConfig.getEvictionPolicy(),
                evictionConfig.getComparatorClassName(), evictionConfig.getComparator());
    }

    @Test
    public void checkEvictionConfig_whenNoneOfTheComparatorAndComparatorClassNameAreSetIfEvictionPolicyIsRandom_forNearCache() {
        EvictionConfig evictionConfig = getEvictionConfig(false, false, EvictionPolicy.RANDOM);
        checkNearCacheEvictionConfig(evictionConfig.getEvictionPolicy(),
                evictionConfig.getComparatorClassName(), evictionConfig.getComparator());
    }

    @Test(expected = InvalidConfigurationException.class)
    public void checkEvictionConfig_whenComparatorClassNameIsSetIfEvictionPolicyIsRandom_forNearCache() {
        EvictionConfig evictionConfig = getEvictionConfig(true, false, EvictionPolicy.RANDOM);
        checkNearCacheEvictionConfig(evictionConfig.getEvictionPolicy(),
                evictionConfig.getComparatorClassName(), evictionConfig.getComparator());
    }

    @Test(expected = InvalidConfigurationException.class)
    public void checkEvictionConfig_whenComparatorIsSetIfEvictionPolicyIsRandom_forNearCache() {
        EvictionConfig evictionConfig = getEvictionConfig(false, true, EvictionPolicy.RANDOM);
        checkNearCacheEvictionConfig(evictionConfig.getEvictionPolicy(),
                evictionConfig.getComparatorClassName(), evictionConfig.getComparator());
    }

    @Test
    public void checkEvictionConfig_whenComparatorClassNameIsSetIfEvictionPolicyIsNotSet_forNearCache() {
        EvictionConfig evictionConfig = getEvictionConfig(true, false);
        checkNearCacheEvictionConfig(evictionConfig.getEvictionPolicy(),
                evictionConfig.getComparatorClassName(), evictionConfig.getComparator());
    }

    @Test
    public void checkEvictionConfig_whenComparatorIsSetIfEvictionPolicyIsNotSet_forNearCache() {
        EvictionConfig evictionConfig = getEvictionConfig(false, true);
        checkNearCacheEvictionConfig(evictionConfig.getEvictionPolicy(),
                evictionConfig.getComparatorClassName(), evictionConfig.getComparator());
    }

    @Test(expected = InvalidConfigurationException.class)
    public void checkEvictionConfig_whenComparatorClassNameIsSetIfEvictionPolicyIsAlsoSet_forNearCache() {
        // default eviction policy is LRU (see EvictionConfig.DEFAULT_EVICTION_POLICY)
        EvictionConfig evictionConfig = getEvictionConfig(true, false, EvictionPolicy.LFU);
        checkNearCacheEvictionConfig(evictionConfig.getEvictionPolicy(),
                evictionConfig.getComparatorClassName(), evictionConfig.getComparator());
    }

    @Test(expected = InvalidConfigurationException.class)
    public void checkEvictionConfig_whenComparatorIsSetIfEvictionPolicyIsAlsoSet_forNearCache() {
        // default eviction policy is LRU (see EvictionConfig.DEFAULT_EVICTION_POLICY)
        EvictionConfig evictionConfig = getEvictionConfig(false, true, EvictionPolicy.LFU);
        checkNearCacheEvictionConfig(evictionConfig.getEvictionPolicy(),
                evictionConfig.getComparatorClassName(), evictionConfig.getComparator());
    }

    @Test(expected = InvalidConfigurationException.class)
    public void checkEvictionConfig_whenNoneOfTheComparatorAndComparatorClassNameAreSetIfEvictionPolicyIsNull() {
        ConfigValidator.checkEvictionConfig(null, null, null, COMMONLY_SUPPORTED_EVICTION_POLICIES);
    }

    @Test
    public void checkEvictionConfig_whenNoneOfTheComparatorAndComparatorClassNameAreSetIfEvictionPolicyIsNull_forNearCache() {
        checkNearCacheEvictionConfig(null, null, null);
    }

    @Test
    public void checkEvictionConfig_withEntryCountMaxSizePolicy_OBJECT() {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaxSizePolicy(MaxSizePolicy.ENTRY_COUNT);

        checkCacheMaxSizePolicy(evictionConfig.getMaxSizePolicy(), InMemoryFormat.OBJECT);
    }

    @Test
    public void checkEvictionConfig_withEntryCountMaxSizePolicy_BINARY() {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaxSizePolicy(MaxSizePolicy.ENTRY_COUNT);

        checkCacheMaxSizePolicy(evictionConfig.getMaxSizePolicy(), InMemoryFormat.BINARY);
    }

    @Test(expected = IllegalArgumentException.class)
    public void checkEvictionConfig_withEntryCountMaxSizePolicy_NATIVE() {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaxSizePolicy(MaxSizePolicy.ENTRY_COUNT);

        checkCacheMaxSizePolicy(evictionConfig.getMaxSizePolicy(), InMemoryFormat.NATIVE);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void checkMapMaxSizePolicyPerInMemoryFormat_when_BINARY() {
        MapConfig mapConfig = new MapConfig();
        mapConfig.setInMemoryFormat(InMemoryFormat.BINARY);
        mapConfig.getEvictionConfig().setMaxSizePolicy(MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);

        checkMapMaxSizePolicyPerInMemoryFormat(mapConfig);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void checkMapMaxSizePolicyPerInMemoryFormat_when_NATIVEY() {
        MapConfig mapConfig = new MapConfig();
        mapConfig.setInMemoryFormat(InMemoryFormat.NATIVE);
        mapConfig.getEvictionConfig().setMaxSizePolicy(MaxSizePolicy.USED_HEAP_SIZE);

        checkMapMaxSizePolicyPerInMemoryFormat(mapConfig);
    }

    private EvictionConfig getEvictionConfig(boolean setComparatorClass, boolean setComparator) {
        return getEvictionConfig(setComparatorClass, setComparator, EvictionConfig.DEFAULT_EVICTION_POLICY);
    }

    private EvictionConfig getEvictionConfig(boolean setComparatorClass, boolean setComparator, EvictionPolicy evictionPolicy) {
        EvictionConfig evictionConfig = new EvictionConfig();
        if (setComparatorClass) {
            evictionConfig.setComparatorClassName("myComparatorClass");
        }
        if (setComparator) {
            evictionConfig.setComparator((o1, o2) -> 0);
        }
        evictionConfig.setEvictionPolicy(evictionPolicy);
        return evictionConfig;
    }

    private static void checkEvictionConfig(EvictionConfig evictionConfig) {
        ConfigValidator.checkEvictionConfig(evictionConfig, COMMONLY_SUPPORTED_EVICTION_POLICIES);
    }
}
