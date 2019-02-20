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

import com.hazelcast.cache.impl.merge.policy.CacheMergePolicyProvider;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.map.merge.MergePolicyProvider;
import com.hazelcast.spi.NodeEngine;
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
import static com.hazelcast.internal.config.ConfigValidator.checkCPSubsystemConfig;
import static com.hazelcast.internal.config.ConfigValidator.checkCacheConfig;
import static com.hazelcast.internal.config.ConfigValidator.checkMapConfig;
import static com.hazelcast.internal.config.ConfigValidator.checkNearCacheNativeMemoryConfig;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ConfigValidatorTest extends HazelcastTestSupport {

    private MergePolicyProvider mapMergePolicyProvider;
    private CacheMergePolicyProvider cacheMergePolicyProvider;

    @Before
    public void setUp() {
        Config config = new Config();
        NodeEngine nodeEngine = Mockito.mock(NodeEngine.class);
        when(nodeEngine.getConfigClassLoader()).thenReturn(config.getClassLoader());

        SplitBrainMergePolicyProvider splitBrainMergePolicyProvider = new SplitBrainMergePolicyProvider(nodeEngine);
        when(nodeEngine.getSplitBrainMergePolicyProvider()).thenReturn(splitBrainMergePolicyProvider);

        mapMergePolicyProvider = new MergePolicyProvider(nodeEngine);
        cacheMergePolicyProvider = new CacheMergePolicyProvider(nodeEngine);
    }

    @Test
    public void testConstructor() {
        assertUtilityConstructor(ConfigValidator.class);
    }

    @Test
    public void checkMapConfig_BINARY() {
        checkMapConfig(getMapConfig(BINARY), mapMergePolicyProvider);
    }

    @Test
    public void checkMapConfig_OBJECT() {
        checkMapConfig(getMapConfig(OBJECT), mapMergePolicyProvider);
    }

    /**
     * Not supported in open source version, so test is expected to throw exception.
     */
    @Test(expected = IllegalArgumentException.class)
    public void checkMapConfig_NATIVE() {
        checkMapConfig(getMapConfig(NATIVE), mapMergePolicyProvider);
    }

    @Test
    @SuppressWarnings("deprecation")
    public void checkMapConfig_withIgnoredConfigMinEvictionCheckMillis() {
        MapConfig mapConfig = getMapConfig(BINARY)
                .setMinEvictionCheckMillis(100);

        checkMapConfig(mapConfig, mapMergePolicyProvider);
    }

    @Test
    @SuppressWarnings("deprecation")
    public void checkMapConfig_withIgnoredConfigEvictionPercentage() {
        MapConfig mapConfig = getMapConfig(BINARY)
                .setEvictionPercentage(50);

        checkMapConfig(mapConfig, mapMergePolicyProvider);
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

        checkCacheConfig(cacheSimpleConfig, cacheMergePolicyProvider);
    }

    @Test(expected = IllegalArgumentException.class)
    public void checkCacheConfig_withEntryCountMaxSizePolicy_NATIVE() {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.ENTRY_COUNT);
        CacheSimpleConfig cacheSimpleConfig = new CacheSimpleConfig()
                .setInMemoryFormat(NATIVE)
                .setEvictionConfig(evictionConfig);

        checkCacheConfig(cacheSimpleConfig, cacheMergePolicyProvider);
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
