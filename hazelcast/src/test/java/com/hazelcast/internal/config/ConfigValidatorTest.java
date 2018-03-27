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
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.internal.cluster.Versions;
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
import static com.hazelcast.internal.config.ConfigValidator.checkCacheConfig;
import static com.hazelcast.internal.config.ConfigValidator.checkMapConfig;
import static com.hazelcast.internal.config.ConfigValidator.checkMergePolicySupportsInMemoryFormat;
import static com.hazelcast.internal.config.ConfigValidator.checkNearCacheNativeMemoryConfig;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ConfigValidatorTest extends HazelcastTestSupport {

    private static final ILogger LOGGER = Logger.getLogger(ConfigValidatorTest.class);

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

    private MapConfig getMapConfig(InMemoryFormat inMemoryFormat) {
        return new MapConfig()
                .setInMemoryFormat(inMemoryFormat);
    }

    @Test
    public void checkCacheConfig_withEntryCountMaxSizePolicy_OBJECT() {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.ENTRY_COUNT);
        CacheSimpleConfig cacheSimpleConfig = new CacheSimpleConfig()
                .setInMemoryFormat(OBJECT)
                .setEvictionConfig(evictionConfig);

        checkCacheConfig(cacheSimpleConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void checkCacheConfig_withEntryCountMaxSizePolicy_NATIVE() {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.ENTRY_COUNT);
        CacheSimpleConfig cacheSimpleConfig = new CacheSimpleConfig()
                .setInMemoryFormat(NATIVE)
                .setEvictionConfig(evictionConfig);

        checkCacheConfig(cacheSimpleConfig);
    }

    @Test
    public void checkNearCacheNativeMemoryConfig_shouldNotNeedNativeMemoryConfig_BINARY_onOS() {
        checkNearCacheNativeMemoryConfig(BINARY, null, false);
    }

    @Test
    public void checkNearCacheNativeMemoryConfig_shouldNotNeedNativeMemoryConfig_BINARY_onEE() {
        checkNearCacheNativeMemoryConfig(BINARY, null, true);
    }

    @Test
    public void checkNearCacheNativeMemoryConfig_shouldNotThrowExceptionWithoutNativeMemoryConfig_NATIVE_onOS() {
        checkNearCacheNativeMemoryConfig(NATIVE, null, false);
    }

    @Test
    public void checkNearCacheNativeMemoryConfig_shouldNotThrowExceptionWithNativeMemoryConfig_NATIVE_onEE() {
        NativeMemoryConfig nativeMemoryConfig = new NativeMemoryConfig()
                .setEnabled(true);

        checkNearCacheNativeMemoryConfig(NATIVE, nativeMemoryConfig, true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void checkNearCacheNativeMemoryConfig_shouldThrowExceptionWithoutNativeMemoryConfig_NATIVE_onEE() {
        checkNearCacheNativeMemoryConfig(NATIVE, null, true);
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
