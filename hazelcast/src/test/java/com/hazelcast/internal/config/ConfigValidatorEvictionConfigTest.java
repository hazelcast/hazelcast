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

package com.hazelcast.internal.config;

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.internal.eviction.EvictableEntryView;
import com.hazelcast.internal.eviction.EvictionPolicyComparator;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.config.ConfigValidator.checkEvictionConfig;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ConfigValidatorEvictionConfigTest extends HazelcastTestSupport {

    @Test
    public void checkEvictionConfig_forMapAndCache() {
        checkEvictionConfig(getEvictionConfig(false, false), false);
    }

    @Test
    public void checkEvictionConfig_forNearCache() {
        checkEvictionConfig(getEvictionConfig(false, false, EvictionPolicy.RANDOM), true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void checkEvictionConfig_withNull() {
        checkEvictionConfig(null, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void checkEvictionConfig_withNull_forNearCache() {
        checkEvictionConfig(null, true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void checkEvictionConfig_whenBothOfComparatorAndComparatorClassNameAreSet() {
        checkEvictionConfig(getEvictionConfig(true, true), false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void checkEvictionConfig_whenBothOfComparatorAndComparatorClassNameAreSet_forNearCache() {
        checkEvictionConfig(getEvictionConfig(true, true), true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void checkEvictionConfig_whenNoneOfTheComparatorAndComparatorClassNameAreSetIfEvictionPolicyIsNone() {
        checkEvictionConfig(getEvictionConfig(false, false, EvictionPolicy.NONE), false);
    }

    @Test
    public void checkEvictionConfig_whenComparatorClassNameIsSetIfEvictionPolicyIsNone() {
        checkEvictionConfig(getEvictionConfig(true, false, EvictionPolicy.NONE), false);
    }

    @Test
    public void checkEvictionConfig_whenComparatorIsSetIfEvictionPolicyIsNone() {
        checkEvictionConfig(getEvictionConfig(false, true, EvictionPolicy.NONE), false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void checkEvictionConfig_whenNoneOfTheComparatorAndComparatorClassNameAreSetIfEvictionPolicyIsRandom() {
        checkEvictionConfig(getEvictionConfig(false, false, EvictionPolicy.RANDOM), false);
    }

    @Test
    public void checkEvictionConfig_whenComparatorClassNameIsSetIfEvictionPolicyIsRandom() {
        checkEvictionConfig(getEvictionConfig(true, false, EvictionPolicy.RANDOM), false);
    }

    @Test
    public void checkEvictionConfig_whenComparatorIsSetIfEvictionPolicyIsRandom() {
        checkEvictionConfig(getEvictionConfig(false, true, EvictionPolicy.RANDOM), false);
    }

    @Test
    public void checkEvictionConfig_whenComparatorClassNameIsSetIfEvictionPolicyIsNotSet() {
        checkEvictionConfig(getEvictionConfig(true, false), false);
    }

    @Test
    public void checkEvictionConfig_whenComparatorIsSetIfEvictionPolicyIsNotSet() {
        checkEvictionConfig(getEvictionConfig(false, true), false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void checkEvictionConfig_whenComparatorClassNameIsSetIfEvictionPolicyIsAlsoSet() {
        // default eviction policy is LRU (see EvictionConfig.DEFAULT_EVICTION_POLICY)
        checkEvictionConfig(getEvictionConfig(true, false, EvictionPolicy.LFU), false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void checkEvictionConfig_whenComparatorIsSetIfEvictionPolicyIsAlsoSet() {
        // default eviction policy is LRU (see EvictionConfig.DEFAULT_EVICTION_POLICY)
        checkEvictionConfig(getEvictionConfig(false, true, EvictionPolicy.LFU), false);
    }

    @Test
    public void checkEvictionConfig_whenNoneOfTheComparatorAndComparatorClassNameAreSetIfEvictionPolicyIsNone_forNearCache() {
        checkEvictionConfig(getEvictionConfig(false, false, EvictionPolicy.NONE), true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void checkEvictionConfig_whenComparatorClassNameIsSetIfEvictionPolicyIsNone_forNearCache() {
        checkEvictionConfig(getEvictionConfig(true, false, EvictionPolicy.NONE), true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void checkEvictionConfig_whenComparatorIsSetIfEvictionPolicyIsNone_forNearCache() {
        checkEvictionConfig(getEvictionConfig(false, true, EvictionPolicy.NONE), true);
    }

    @Test
    public void checkEvictionConfig_whenNoneOfTheComparatorAndComparatorClassNameAreSetIfEvictionPolicyIsRandom_forNearCache() {
        checkEvictionConfig(getEvictionConfig(false, false, EvictionPolicy.RANDOM), true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void checkEvictionConfig_whenComparatorClassNameIsSetIfEvictionPolicyIsRandom_forNearCache() {
        checkEvictionConfig(getEvictionConfig(true, false, EvictionPolicy.RANDOM), true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void checkEvictionConfig_whenComparatorIsSetIfEvictionPolicyIsRandom_forNearCache() {
        checkEvictionConfig(getEvictionConfig(false, true, EvictionPolicy.RANDOM), true);
    }

    @Test
    public void checkEvictionConfig_whenComparatorClassNameIsSetIfEvictionPolicyIsNotSet_forNearCache() {
        checkEvictionConfig(getEvictionConfig(true, false), true);
    }

    @Test
    public void checkEvictionConfig_whenComparatorIsSetIfEvictionPolicyIsNotSet_forNearCache() {
        checkEvictionConfig(getEvictionConfig(false, true), true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void checkEvictionConfig_whenComparatorClassNameIsSetIfEvictionPolicyIsAlsoSet_forNearCache() {
        // default eviction policy is LRU (see EvictionConfig.DEFAULT_EVICTION_POLICY)
        checkEvictionConfig(getEvictionConfig(true, false, EvictionPolicy.LFU), true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void checkEvictionConfig_whenComparatorIsSetIfEvictionPolicyIsAlsoSet_forNearCache() {
        // default eviction policy is LRU (see EvictionConfig.DEFAULT_EVICTION_POLICY)
        checkEvictionConfig(getEvictionConfig(false, true, EvictionPolicy.LFU), true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void checkEvictionConfig_whenNoneOfTheComparatorAndComparatorClassNameAreSetIfEvictionPolicyIsNull() {
        checkEvictionConfig(null, null, null, false);
    }

    @Test
    public void checkEvictionConfig_whenNoneOfTheComparatorAndComparatorClassNameAreSetIfEvictionPolicyIsNull_forNearCache() {
        checkEvictionConfig(null, null, null, true);
    }

    @Test
    public void checkEvictionConfig_withEntryCountMaxSizePolicy_OBJECT() {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.ENTRY_COUNT);

        checkEvictionConfig(InMemoryFormat.OBJECT, evictionConfig);
    }

    @Test
    public void checkEvictionConfig_withEntryCountMaxSizePolicy_BINARY() {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.ENTRY_COUNT);

        checkEvictionConfig(InMemoryFormat.BINARY, evictionConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void checkEvictionConfig_withEntryCountMaxSizePolicy_NATIVE() {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.ENTRY_COUNT);

        checkEvictionConfig(InMemoryFormat.NATIVE, evictionConfig);
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
            evictionConfig.setComparator(new EvictionPolicyComparator() {
                @Override
                public int compare(EvictableEntryView e1, EvictableEntryView e2) {
                    return 0;
                }

                public int compare(Object o1, Object o2) {
                    return 0;
                }
            });
        }
        evictionConfig.setEvictionPolicy(evictionPolicy);
        return evictionConfig;
    }
}
