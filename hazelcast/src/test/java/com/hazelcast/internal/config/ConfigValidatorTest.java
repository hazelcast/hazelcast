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

import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.merge.HigherHitsMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergePolicyProvider;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.internal.config.ConfigValidator.checkCPSubsystemConfig;
import static com.hazelcast.internal.config.ConfigValidator.checkCacheConfig;
import static com.hazelcast.internal.config.ConfigValidator.checkMapConfig;
import static com.hazelcast.internal.config.ConfigValidator.checkNearCacheNativeMemoryConfig;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ConfigValidatorTest extends HazelcastTestSupport {

    private HazelcastProperties properties;
    private NativeMemoryConfig nativeMemoryConfig;
    private SplitBrainMergePolicyProvider splitBrainMergePolicyProvider;
    private ILogger logger;

    @Before
    public void setUp() {
        Config config = new Config();
        nativeMemoryConfig = config.getNativeMemoryConfig();
        NodeEngine nodeEngine = Mockito.mock(NodeEngine.class);
        when(nodeEngine.getConfigClassLoader()).thenReturn(config.getClassLoader());

        splitBrainMergePolicyProvider = new SplitBrainMergePolicyProvider(config.getClassLoader());
        when(nodeEngine.getSplitBrainMergePolicyProvider()).thenReturn(splitBrainMergePolicyProvider);

        properties = nodeEngine.getProperties();
        logger = nodeEngine.getLogger(MapConfig.class);
    }

    @Test
    public void testConstructor() {
        assertUtilityConstructor(ConfigValidator.class);
    }

    @Test
    public void checkMapConfig_BINARY() {
        checkMapConfig(getMapConfig(BINARY), nativeMemoryConfig, splitBrainMergePolicyProvider, properties, logger);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void checkMapConfig_fails_with_merge_policy_which_requires_per_entry_stats_enabled() {
        checkMapConfig(getMapConfig(BINARY).setPerEntryStatsEnabled(false),
                nativeMemoryConfig, splitBrainMergePolicyProvider, properties, logger);
    }

    @Test
    public void checkMapConfig_OBJECT() {
        checkMapConfig(getMapConfig(OBJECT), nativeMemoryConfig, splitBrainMergePolicyProvider, properties, logger);
    }

    /**
     * Not supported in open source version, so test is expected to throw exception.
     */
    @Test(expected = InvalidConfigurationException.class)
    public void checkMapConfig_NATIVE() {
        checkMapConfig(getMapConfig(NATIVE), nativeMemoryConfig, splitBrainMergePolicyProvider, properties, logger);
    }

    /**
     * Not supported in open source version, so test is expected to throw exception.
     */
    @Test(expected = InvalidConfigurationException.class)
    public void checkMapConfig_TieredStore() {
        checkMapConfig(getMapConfig(true), nativeMemoryConfig, splitBrainMergePolicyProvider, properties, logger);
    }

    private MapConfig getMapConfig(InMemoryFormat inMemoryFormat) {
        MapConfig mapConfig = new MapConfig()
                .setInMemoryFormat(inMemoryFormat)
                .setPerEntryStatsEnabled(true);
        mapConfig.getMergePolicyConfig()
                .setPolicy(HigherHitsMergePolicy.class.getName());
        return mapConfig;
    }

    private MapConfig getMapConfig(boolean tieredStoreEnabled) {
        MapConfig mapConfig = new MapConfig();
        mapConfig.getTieredStoreConfig().setEnabled(tieredStoreEnabled);
        return mapConfig;
    }

    @Test
    public void checkCacheConfig_withEntryCountMaxSizePolicy_OBJECT() {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaxSizePolicy(MaxSizePolicy.ENTRY_COUNT);
        CacheSimpleConfig cacheSimpleConfig = new CacheSimpleConfig()
                .setInMemoryFormat(OBJECT)
                .setEvictionConfig(evictionConfig);

        checkCacheConfig(cacheSimpleConfig, splitBrainMergePolicyProvider);
    }

    @Test(expected = IllegalArgumentException.class)
    public void checkCacheConfig_withEntryCountMaxSizePolicy_NATIVE() {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaxSizePolicy(MaxSizePolicy.ENTRY_COUNT);
        CacheSimpleConfig cacheSimpleConfig = new CacheSimpleConfig()
                .setInMemoryFormat(NATIVE)
                .setEvictionConfig(evictionConfig);

        checkCacheConfig(cacheSimpleConfig, splitBrainMergePolicyProvider);
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

    @Test(expected = InvalidConfigurationException.class)
    public void checkNearCacheNativeMemoryConfig_shouldThrowExceptionWithoutNativeMemoryConfig_NATIVE_onEE() {
        checkNearCacheNativeMemoryConfig(NATIVE, null, true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidationFails_whenGroupSizeSetCPMemberCountNotSet() {
        CPSubsystemConfig config = new CPSubsystemConfig();
        config.setGroupSize(3);

        checkCPSubsystemConfig(config);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidationFails_whenGroupSizeGreaterThanCPMemberCount() {
        CPSubsystemConfig config = new CPSubsystemConfig();
        config.setGroupSize(5);
        config.setCPMemberCount(3);

        checkCPSubsystemConfig(config);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidationFails_whenSessionHeartbeatIntervalGreaterThanSessionTTL() {
        CPSubsystemConfig config = new CPSubsystemConfig();
        config.setSessionTimeToLiveSeconds(5);
        config.setSessionHeartbeatIntervalSeconds(10);

        checkCPSubsystemConfig(config);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidationFails_whenSessionTTLGreaterThanMissingCPMemberAutoRemovalSeconds() {
        CPSubsystemConfig config = new CPSubsystemConfig();
        config.setMissingCPMemberAutoRemovalSeconds(5);
        config.setSessionTimeToLiveSeconds(10);

        checkCPSubsystemConfig(config);
    }
}
