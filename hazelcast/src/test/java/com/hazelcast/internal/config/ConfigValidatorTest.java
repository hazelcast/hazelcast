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

package com.hazelcast.internal.config;

import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.eviction.EvictableEntryView;
import com.hazelcast.internal.eviction.EvictionPolicyComparator;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.merge.MergePolicyProvider;
import com.hazelcast.map.merge.PutIfAbsentMapMergePolicy;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.merge.PutIfAbsentMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergePolicyProvider;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy.CACHE_ON_UPDATE;
import static com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy.INVALIDATE;
import static com.hazelcast.internal.config.ConfigValidator.checkCacheConfig;
import static com.hazelcast.internal.config.ConfigValidator.checkEvictionConfig;
import static com.hazelcast.internal.config.ConfigValidator.checkMapConfig;
import static com.hazelcast.internal.config.ConfigValidator.checkMergePolicySupportsInMemoryFormat;
import static com.hazelcast.internal.config.ConfigValidator.checkNearCacheConfig;
import static com.hazelcast.internal.config.ConfigValidator.checkNearCacheNativeMemoryConfig;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ConfigValidatorTest extends HazelcastTestSupport {

    private static final ILogger LOGGER = Logger.getLogger(ConfigValidatorTest.class);

    private static final String MAP_NAME = "default";

    private MergePolicyProvider mapMergePolicyProvider;

    @Before
    public void setUp() {
        Config config = new Config();
        NodeEngine nodeEngine = Mockito.mock(NodeEngine.class);
        when(nodeEngine.getConfigClassLoader()).thenReturn(config.getClassLoader());

        SplitBrainMergePolicyProvider splitBrainMergePolicyProvider = new SplitBrainMergePolicyProvider(nodeEngine);
        when(nodeEngine.getSplitBrainMergePolicyProvider()).thenReturn(splitBrainMergePolicyProvider);

        mapMergePolicyProvider = new MergePolicyProvider(nodeEngine);
    }

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
        checkNearCacheConfig(MAP_NAME, getNearCacheConfig(BINARY), null, false);
    }

    @Test
    public void checkNearCacheConfig_OBJECT() {
        checkNearCacheConfig(MAP_NAME, getNearCacheConfig(OBJECT), null, false);
    }

    /**
     * Not supported in open source version, so test is expected to throw exception.
     */
    @Test(expected = IllegalArgumentException.class)
    public void checkNearCacheConfig_NATIVE() {
        checkNearCacheConfig(MAP_NAME, getNearCacheConfig(NATIVE), null, false);
    }

    /**
     * Not supported client configuration, so test is expected to throw exception.
     */
    @Test(expected = IllegalArgumentException.class)
    public void checkNearCacheConfig_withUnsupportedClientConfig() {
        checkNearCacheConfig(MAP_NAME, getNearCacheConfig(BINARY), null, true);
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

    @Test(expected = IllegalArgumentException.class)
    public void checkCacheConfig_whenNATIVEAndEntryCountMaxSizePolicy() {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.ENTRY_COUNT);
        CacheSimpleConfig cacheSimpleConfig = new CacheSimpleConfig()
                .setInMemoryFormat(NATIVE)
                .setEvictionConfig(evictionConfig);

        checkCacheConfig(cacheSimpleConfig);
    }

    @Test
    public void checkCacheConfig_whenOBJECTAndEntryCountMaxSizePolicy() {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.ENTRY_COUNT);
        CacheSimpleConfig cacheSimpleConfig = new CacheSimpleConfig()
                .setInMemoryFormat(OBJECT)
                .setEvictionConfig(evictionConfig);

        checkCacheConfig(cacheSimpleConfig);
    }

    @Test
    public void checkNearCacheConfig_withPreLoaderConfig_onClients() {
        NearCacheConfig nearCacheConfig = getNearCacheConfig(BINARY)
                .setCacheLocalEntries(false);
        nearCacheConfig.getPreloaderConfig()
                .setEnabled(true)
                .setStoreInitialDelaySeconds(1)
                .setStoreInitialDelaySeconds(1);

        checkNearCacheConfig(MAP_NAME, nearCacheConfig, null, true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void checkNearCacheConfig_withPreloaderConfig_onMembers() {
        NearCacheConfig nearCacheConfig = getNearCacheConfig(BINARY);
        nearCacheConfig.getPreloaderConfig()
                .setEnabled(true)
                .setStoreInitialDelaySeconds(1)
                .setStoreInitialDelaySeconds(1);

        checkNearCacheConfig(MAP_NAME, nearCacheConfig, null, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void checkNearCacheConfig_withLocalUpdatePolicy_CACHE_ON_UPDATE() {
        NearCacheConfig nearCacheConfig = new NearCacheConfig()
                .setLocalUpdatePolicy(CACHE_ON_UPDATE);

        checkNearCacheConfig(MAP_NAME, nearCacheConfig, null, false);
    }

    @Test
    public void checkNearCacheConfig_withLocalUpdatePolicy_INVALIDATE() {
        NearCacheConfig nearCacheConfig = new NearCacheConfig()
                .setLocalUpdatePolicy(INVALIDATE);

        checkNearCacheConfig(MAP_NAME, nearCacheConfig, null, false);
    }

    @Test
    public void should_not_need_native_memory_config_when_on_heap_memory_used_on_os() {
        checkNearCacheNativeMemoryConfig(InMemoryFormat.BINARY, null, false);
    }

    @Test
    public void should_not_need_native_memory_config_when_on_heap_memory_used_on_ee() {
        checkNearCacheNativeMemoryConfig(InMemoryFormat.BINARY, null, true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void should_throw_exception_without_native_memory_config_when_native_memory_used_on_ee() {
        checkNearCacheNativeMemoryConfig(InMemoryFormat.NATIVE, null, true);
    }

    @Test
    public void should_not_throw_exception_without_native_memory_config_when_native_memory_used_on_os() {
        checkNearCacheNativeMemoryConfig(InMemoryFormat.NATIVE, null, false);
    }

    @Test
    public void should_not_throw_exception_with_native_memory_config_when_native_memory_used_on_ee() {
        NativeMemoryConfig nativeMemoryConfig = new NativeMemoryConfig()
                .setEnabled(true);

        checkNearCacheNativeMemoryConfig(InMemoryFormat.NATIVE, nativeMemoryConfig, true);
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

                @SuppressWarnings("ComparatorMethodParameterNotUsed")
                public int compare(Object o1, Object o2) {
                    return 0;
                }
            });
        }
        evictionConfig.setEvictionPolicy(evictionPolicy);
        return evictionConfig;
    }

    @Test
    public void testCheckMergePolicySupportsInMemoryFormat_withMergePolicy_with310_OBJECT() {
        Object mergePolicy = mapMergePolicyProvider.getMergePolicy(PutIfAbsentMergePolicy.class.getName());
        assertTrue(checkMergePolicySupportsInMemoryFormat("myMap", mergePolicy, OBJECT, Versions.V3_10, false, LOGGER));
    }

    @Test
    public void testCheckMergePolicySupportsInMemoryFormat_withLegacyMergePolicy_with310_OBJECT() {
        Object legacyMergePolicy = mapMergePolicyProvider.getMergePolicy(PutIfAbsentMapMergePolicy.class.getName());
        assertTrue(checkMergePolicySupportsInMemoryFormat("myMap", legacyMergePolicy, OBJECT, Versions.V3_10, false, LOGGER));
    }

    @Test
    public void testCheckMergePolicySupportsInMemoryFormat_withMergePolicy_with310_NATIVE() {
        Object mergePolicy = mapMergePolicyProvider.getMergePolicy(PutIfAbsentMergePolicy.class.getName());
        assertTrue(checkMergePolicySupportsInMemoryFormat("myMap", mergePolicy, NATIVE, Versions.V3_10, false, LOGGER));
    }

    /**
     * A legacy merge policy cannot merge NATIVE maps.
     */
    @Test
    public void testCheckMergePolicySupportsInMemoryFormat_withLegacyMergePolicy_with310_NATIVE() {
        Object legacyMergePolicy = mapMergePolicyProvider.getMergePolicy(PutIfAbsentMapMergePolicy.class.getName());
        assertFalse(checkMergePolicySupportsInMemoryFormat("myMap", legacyMergePolicy, NATIVE, Versions.V3_10, false, LOGGER));
    }

    /**
     * A legacy merge policy cannot merge NATIVE maps.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testCheckMergePolicySupportsInMemoryFormat_withLegacyMergePolicy_with310_NATIVE_failFast() {
        Object legacyMergePolicy = mapMergePolicyProvider.getMergePolicy(PutIfAbsentMapMergePolicy.class.getName());
        checkMergePolicySupportsInMemoryFormat("myMap", legacyMergePolicy, NATIVE, Versions.V3_10, true, LOGGER);
    }

    @Test
    public void testCheckMergePolicySupportsInMemoryFormat_withMergePolicy_with39_OBJECT() {
        Object mergePolicy = mapMergePolicyProvider.getMergePolicy(PutIfAbsentMergePolicy.class.getName());
        assertTrue(checkMergePolicySupportsInMemoryFormat("myMap", mergePolicy, OBJECT, Versions.V3_9, false, LOGGER));
    }

    @Test
    public void testCheckMergePolicySupportsInMemoryFormat_withLegacyMergePolicy_with39_OBJECT() {
        Object legacyMergePolicy = mapMergePolicyProvider.getMergePolicy(PutIfAbsentMapMergePolicy.class.getName());
        assertTrue(checkMergePolicySupportsInMemoryFormat("myMap", legacyMergePolicy, OBJECT, Versions.V3_9, false, LOGGER));
    }

    /**
     * A 3.9 cluster cannot merge NATIVE maps.
     */
    @Test
    public void testCheckMergePolicySupportsInMemoryFormat_withMergePolicy_with39_NATIVE() {
        Object mergePolicy = mapMergePolicyProvider.getMergePolicy(PutIfAbsentMergePolicy.class.getName());
        assertFalse(checkMergePolicySupportsInMemoryFormat("myMap", mergePolicy, NATIVE, Versions.V3_9, false, LOGGER));
    }

    /**
     * A 3.9 cluster cannot merge NATIVE maps.
     */
    @Test
    public void testCheckMergePolicySupportsInMemoryFormat_withLegacyMergePolicy_with39_NATIVE() {
        Object legacyMergePolicy = mapMergePolicyProvider.getMergePolicy(PutIfAbsentMapMergePolicy.class.getName());
        assertFalse(checkMergePolicySupportsInMemoryFormat("myMap", legacyMergePolicy, NATIVE, Versions.V3_9, false, LOGGER));
    }

    /**
     * A 3.9 cluster cannot merge NATIVE maps, but will not throw an exception, even if fail-fast is configured.
     * <p>
     * This is for compatibility with existing setups.
     */
    @Test
    public void testCheckMergePolicySupportsInMemoryFormat_withMergePolicy_with39_NATIVE_failFast() {
        Object mergePolicy = mapMergePolicyProvider.getMergePolicy(PutIfAbsentMergePolicy.class.getName());
        assertFalse(checkMergePolicySupportsInMemoryFormat("myMap", mergePolicy, NATIVE, Versions.V3_9, true, LOGGER));
    }

    /**
     * A 3.9 cluster cannot merge NATIVE maps, but will not throw an exception, even if fail-fast is configured.
     * <p>
     * This is for compatibility with existing setups.
     */
    @Test
    public void testCheckMergePolicySupportsInMemoryFormat_withLegacyMergePolicy_with39_NATIVE_failFast() {
        Object legacyMergePolicy = mapMergePolicyProvider.getMergePolicy(PutIfAbsentMapMergePolicy.class.getName());
        assertFalse(checkMergePolicySupportsInMemoryFormat("myMap", legacyMergePolicy, NATIVE, Versions.V3_9, true, LOGGER));
    }
}
