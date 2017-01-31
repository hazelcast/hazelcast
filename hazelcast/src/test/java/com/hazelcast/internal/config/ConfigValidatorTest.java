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

package com.hazelcast.internal.config;

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.internal.eviction.EvictableEntryView;
import com.hazelcast.internal.eviction.EvictionPolicyComparator;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy.CACHE_ON_UPDATE;
import static com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy.INVALIDATE;
import static com.hazelcast.internal.config.ConfigValidator.checkEvictionConfig;
import static com.hazelcast.internal.config.ConfigValidator.checkMapConfig;
import static com.hazelcast.internal.config.ConfigValidator.checkNearCacheConfig;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ConfigValidatorTest extends HazelcastTestSupport {

    private static final String MAP_NAME = "default";

    @Test
    public void testConstructor() {
        assertUtilityConstructor(ConfigValidator.class);
    }

    @Test
    public void checkMapConfig_BINARY() {
        checkMapConfig(getMapConfig(BINARY));
    }

    @Test
    public void checkMapConfig_OBJECT() {
        checkMapConfig(getMapConfig(OBJECT));
    }

    /**
     * Not supported in open source version, so test is expected to throw exception.
     */
    @Test(expected = IllegalArgumentException.class)
    public void checkMapConfig_NATIVE() {
        checkMapConfig(getMapConfig(NATIVE));
    }

    @Test
    @SuppressWarnings("deprecation")
    public void checkMapConfig_withIgnoredConfigMinEvictionCheckMillis() {
        MapConfig mapConfig = getMapConfig(BINARY)
                .setMinEvictionCheckMillis(100);
        checkMapConfig(mapConfig);
    }

    @Test
    @SuppressWarnings("deprecation")
    public void checkMapConfig_withIgnoredConfigEvictionPercentage() {
        MapConfig mapConfig = getMapConfig(BINARY)
                .setEvictionPercentage(50);
        checkMapConfig(mapConfig);
    }

    @Test
    public void checkNearCacheConfig_BINARY() {
        checkNearCacheConfig(MAP_NAME, getNearCacheConfig(BINARY), false);
    }

    @Test
    public void checkNearCacheConfig_OBJECT() {
        checkNearCacheConfig(MAP_NAME, getNearCacheConfig(OBJECT), false);
    }

    /**
     * Not supported in open source version, so test is expected to throw exception.
     */
    @Test(expected = IllegalArgumentException.class)
    public void checkNearCacheConfig_NATIVE() {
        checkNearCacheConfig(MAP_NAME, getNearCacheConfig(NATIVE), false);
    }

    /**
     * Not supported client configuration, so test is expected to throw exception.
     */
    @Test(expected = IllegalArgumentException.class)
    public void checkNearCacheConfig_withUnsupportedClientConfig() {
        checkNearCacheConfig(MAP_NAME, getNearCacheConfig(BINARY), true);
    }

    @Test
    public void checkEvictionConfig_forMapAndCache() {
        checkEvictionConfig(getEvictionConfig(false, false), false);
    }

    @Test
    public void checkEvictionConfig_forNearCache() {
        checkEvictionConfig(getEvictionConfig(false, false, EvictionPolicy.RANDOM), true);
    }

    @SuppressWarnings("ConstantConditions")
    @Test(expected = IllegalArgumentException.class)
    public void checkEvictionConfig_withNull() {
        checkEvictionConfig(null, false);
    }

    @SuppressWarnings("ConstantConditions")
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
        // default eviction policy is `LRU`. See `EvictionConfig.DEFAULT_EVICTION_POLICY`
        checkEvictionConfig(getEvictionConfig(true, false, EvictionPolicy.LFU), false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void checkEvictionConfig_whenComparatorIsSetIfEvictionPolicyIsAlsoSet() {
        // default eviction policy is `LRU`. See `EvictionConfig.DEFAULT_EVICTION_POLICY`
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
        // default eviction policy is `LRU`. See `EvictionConfig.DEFAULT_EVICTION_POLICY`
        checkEvictionConfig(getEvictionConfig(true, false, EvictionPolicy.LFU), true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void checkEvictionConfig_whenComparatorIsSetIfEvictionPolicyIsAlsoSet_forNearCache() {
        // default eviction policy is `LRU`. See `EvictionConfig.DEFAULT_EVICTION_POLICY`
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
    public void test_checkNearCacheConfig_withPreLoaderConfig_onClients() {
        NearCacheConfig nearCacheConfig = getNearCacheConfig(BINARY)
                .setCacheLocalEntries(false);
        nearCacheConfig.getPreloaderConfig()
                .setEnabled(true)
                .setStoreInitialDelaySeconds(1)
                .setStoreInitialDelaySeconds(1);

        checkNearCacheConfig(MAP_NAME, nearCacheConfig, true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_checkNearCacheConfig_withPreloader_onMembers() {
        NearCacheConfig nearCacheConfig = getNearCacheConfig(BINARY);
        nearCacheConfig.getPreloaderConfig()
                .setEnabled(true)
                .setStoreInitialDelaySeconds(1)
                .setStoreInitialDelaySeconds(1);

        checkNearCacheConfig(MAP_NAME, nearCacheConfig, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void not_supports_near_cache_localUpdatePolicy_CACHE_ON_UPDATE() {
        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setLocalUpdatePolicy(CACHE_ON_UPDATE);

        checkNearCacheConfig(MAP_NAME, nearCacheConfig, false);
    }

    @Test
    public void supports_near_cache_localUpdatePolicy_INVALIDATE() {
        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setLocalUpdatePolicy(INVALIDATE);

        checkNearCacheConfig(MAP_NAME, nearCacheConfig, false);
    }

    private MapConfig getMapConfig(InMemoryFormat inMemoryFormat) {
        return new MapConfig()
                .setInMemoryFormat(inMemoryFormat);
    }

    private NearCacheConfig getNearCacheConfig(InMemoryFormat inMemoryFormat) {
        return new NearCacheConfig()
                .setInMemoryFormat(inMemoryFormat)
                .setCacheLocalEntries(true);
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
                @SuppressWarnings("ComparatorMethodParameterNotUsed")
                public int compare(EvictableEntryView e1, EvictableEntryView e2) {
                    return 0;
                }

                @Override
                @SuppressWarnings("ComparatorMethodParameterNotUsed")
                public int compare(Object o1, Object o2) {
                    return 0;
                }
            });
        }
        evictionConfig.setEvictionPolicy(evictionPolicy);
        return evictionConfig;
    }
}
