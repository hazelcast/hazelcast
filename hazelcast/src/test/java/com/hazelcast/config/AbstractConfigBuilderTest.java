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

import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;

/**
 * Abstract class defining the common test cases for XML and YAML
 * based configuration tests.
 * <p/>
 * All common test cases should be defined in this class to guarantee
 * compilation error if either YAML or XML configuration misses to cover
 * a common case.
 * <p/>
 * For specific test cases, see {@link XmlOnlyConfigBuilderTest} and
 * {@link YamlOnlyConfigBuilderTest}.
 *
 * @see XMLConfigBuilderTest
 * @see YamlConfigBuilderTest
 */
public abstract class AbstractConfigBuilderTest extends HazelcastTestSupport {
    @Test
    public abstract void testConfigurationURL() throws Exception;

    @Test
    public abstract void testConfigurationWithFileName() throws Exception;

    @Test(expected = IllegalArgumentException.class)
    public abstract void testConfiguration_withNullInputStream();

    @Test(expected = InvalidConfigurationException.class)
    public abstract void testInvalidRootElement();

    @Test(expected = InvalidConfigurationException.class)
    public abstract void testJoinValidation();

    @Test
    public abstract void testSecurityInterceptorConfig();

    @Test
    public abstract void readAwsConfig();

    @Test
    public abstract void readGcpConfig();

    @Test
    public abstract void readAzureConfig();

    @Test
    public abstract void readKubernetesConfig();

    @Test
    public abstract void readEurekaConfig();

    @Test
    public abstract void readDiscoveryConfig();

    @Test
    public abstract void testSSLConfig();

    @Test
    public abstract void testSymmetricEncryptionConfig();

    @Test
    public abstract void readPortCount();

    @Test
    public abstract void readPortAutoIncrement();

    @Test
    public abstract void networkReuseAddress();

    @Test
    public abstract void readSemaphoreConfig();

    @Test
    public abstract void readQueueConfig();

    @Test
    public abstract void readListConfig();

    @Test
    public abstract void readSetConfig();

    @Test
    public abstract void readLockConfig();

    @Test
    public abstract void readReliableTopic();

    @Test
    public abstract void readRingbuffer();

    @Test
    public abstract void readAtomicLong();

    @Test
    public abstract void readAtomicReference();

    @Test
    public abstract void readCountDownLatch();

    @Test
    public abstract void testCaseInsensitivityOfSettings();

    @Test
    public abstract void testManagementCenterConfig();

    @Test
    public abstract void testManagementCenterConfigComplex();

    @Test
    public abstract void testNullManagementCenterConfig();

    @Test
    public abstract void testEmptyManagementCenterConfig();

    @Test
    public abstract void testNotEnabledManagementCenterConfig();

    @Test
    public abstract void testNotEnabledWithURLManagementCenterConfig();

    @Test
    public abstract void testManagementCenterConfigComplexDisabledMutualAuth();

    @Test
    public abstract void testMapStoreInitialModeLazy();

    @Test
    public abstract void testMapConfig_minEvictionCheckMillis();

    @Test
    public abstract void testMapConfig_minEvictionCheckMillis_defaultValue();

    @Test
    public abstract void testMapConfig_preprocessingPolicy();

    @Test
    public abstract void testMapConfig_preprocessingPolicy_defaultValue();

    @Test
    public abstract void testMapConfig_evictions();

    @Test
    public abstract void testMapConfig_optimizeQueries();

    @Test
    public abstract void testMapConfig_cacheValueConfig_defaultValue();

    @Test
    public abstract void testMapConfig_cacheValueConfig_never();

    @Test
    public abstract void testMapConfig_cacheValueConfig_always();

    @Test
    public abstract void testMapConfig_cacheValueConfig_indexOnly();

    @Test
    public abstract void testMapStoreInitialModeEager();

    @Test
    public abstract void testMapStoreWriteBatchSize();

    @Test
    public abstract void testMapStoreConfig_writeCoalescing_whenDefault();

    @Test
    public abstract void testMapStoreConfig_writeCoalescing_whenSetFalse();

    @Test
    public abstract void testMapStoreConfig_writeCoalescing_whenSetTrue();

    @Test
    public abstract void testNearCacheInMemoryFormat();

    @Test
    public abstract void testNearCacheInMemoryFormatNative_withKeysByReference();

    @Test
    public abstract void testNearCacheEvictionPolicy();

    @Test
    public abstract void testPartitionGroupZoneAware();

    @Test
    public abstract void testPartitionGroupSPI();

    @Test
    public abstract void testPartitionGroupMemberGroups();

    @Test
    public abstract void testNearCacheFullConfig();

    @Test
    public abstract void testMapWanReplicationRef();

    @Test
    public abstract void testWanReplicationConfig();

    @Test
    public abstract void testDefaultOfPersistWanReplicatedDataIsFalse();

    @Test
    public abstract void testWanReplicationSyncConfig();

    @Test
    public abstract void testMapEventJournalConfig();

    @Test
    public abstract void testMapMerkleTreeConfig();

    @Test
    public abstract void testCacheEventJournalConfig();

    @Test
    public abstract void testFlakeIdGeneratorConfig();

    @Test(expected = InvalidConfigurationException.class)
    public abstract void testParseExceptionIsNotSwallowed();

    @Test
    public abstract void setMapStoreConfigImplementationTest();

    @Test
    public abstract void testMapPartitionLostListenerConfig();

    @Test
    public abstract void testMapPartitionLostListenerConfigReadOnly();

    @Test
    public abstract void testCachePartitionLostListenerConfig();

    @Test
    public abstract void testCachePartitionLostListenerConfigReadOnly();

    protected abstract Config buildConfig(String xml);

    @Test
    public abstract void readMulticastConfig();

    @Test
    public abstract void testWanConfig();

    @Test
    public abstract void testQuorumConfig();

    @Test
    public abstract void testQuorumListenerConfig();

    @Test(expected = ConfigurationException.class)
    public abstract void testQuorumConfig_whenClassNameAndRecentlyActiveQuorumDefined_exceptionIsThrown();

    @Test(expected = ConfigurationException.class)
    public abstract void testQuorumConfig_whenClassNameAndProbabilisticQuorumDefined_exceptionIsThrown();

    @Test(expected = InvalidConfigurationException.class)
    public abstract void testQuorumConfig_whenBothBuiltinQuorumsDefined_exceptionIsThrown();

    @Test
    public abstract void testQuorumConfig_whenRecentlyActiveQuorum_withDefaultValues();

    @Test
    public abstract void testQuorumConfig_whenRecentlyActiveQuorum_withCustomValues();

    @Test
    public abstract void testQuorumConfig_whenProbabilisticQuorum_withDefaultValues();

    @Test
    public abstract void testQuorumConfig_whenProbabilisticQuorum_withCustomValues();

    @Test
    public abstract void testCacheConfig();

    @Test
    public abstract void testExecutorConfig();

    @Test
    public abstract void testDurableExecutorConfig();

    @Test
    public abstract void testScheduledExecutorConfig();

    @Test
    public abstract void testCardinalityEstimatorConfig();

    @Test(expected = InvalidConfigurationException.class)
    public abstract void testCardinalityEstimatorConfigWithInvalidMergePolicy();

    @Test
    public abstract void testPNCounterConfig();

    @Test
    public abstract void testMultiMapConfig();

    @Test
    public abstract void testReplicatedMapConfig();

    @Test
    public abstract void testListConfig();

    @Test
    public abstract void testSetConfig();

    @Test
    public abstract void testMapConfig();

    @Test
    public abstract void testIndexesConfig();

    @Test
    public abstract void testAttributeConfig();

    @Test(expected = IllegalArgumentException.class)
    public abstract void testAttributeConfig_noName_emptyTag();

    @Test(expected = IllegalArgumentException.class)
    public abstract void testAttributeConfig_noName_singleTag();

    @Test(expected = IllegalArgumentException.class)
    public abstract void testAttributeConfig_noExtractor();

    @Test(expected = IllegalArgumentException.class)
    public abstract void testAttributeConfig_emptyExtractor();

    @Test
    public abstract void testQueryCacheFullConfig();

    @Test
    public abstract void testMapQueryCachePredicate();

    @Test
    public abstract void testLiteMemberConfig();

    @Test
    public abstract void testNonLiteMemberConfig();

    @Test(expected = InvalidConfigurationException.class)
    public abstract void testNonLiteMemberConfigWithoutEnabledField();

    @Test(expected = InvalidConfigurationException.class)
    public abstract void testInvalidLiteMemberConfig();

    @Test(expected = InvalidConfigurationException.class)
    public abstract void testDuplicateLiteMemberConfig();

    @Test
    public abstract void testMapNativeMaxSizePolicy();

    @Test
    public abstract void testInstanceName();

    @Test
    public abstract void testUserCodeDeployment();

    @Test
    public abstract void testCRDTReplicationConfig();

    @Test
    public abstract void testGlobalSerializer();

    @Test
    public abstract void testJavaSerializationFilter();

    @Test
    public abstract void testHotRestart();

    @Test
    public abstract void testMapEvictionPolicyClassName();

    @Test
    public abstract void testMapEvictionPolicyIsSelected_whenEvictionPolicySet();

    @Test
    public abstract void testCachePermission();

    @Test
    public abstract void testConfigPermission();

    @Test
    public abstract void testAllPermissionsCovered();

    @Test(expected = InvalidConfigurationException.class)
    public abstract void testCacheConfig_withInvalidEvictionConfig_failsFast();

    @Test(expected = InvalidConfigurationException.class)
    public abstract void testCacheConfig_withNativeInMemoryFormat_failsFastInOSS();

    @Test(expected = InvalidConfigurationException.class)
    public abstract void testMemberAddressProvider_classNameIsMandatory();

    @Test
    public abstract void testMemberAddressProviderEnabled();

    @Test
    public abstract void testMemberAddressProviderEnabled_withProperties();

    @Test
    public abstract void testFailureDetector_withProperties();

    @Test
    public abstract void testHandleMemberAttributes();

    @Test
    public abstract void testMemcacheProtocolEnabled();

    @Test
    public abstract void testRestApiDefaults();

    @Test
    public abstract void testRestApiEndpointGroups();

    @Test(expected = InvalidConfigurationException.class)
    public abstract void testUnknownRestApiEndpointGroup();

    protected static void assertAwsConfig(AwsConfig aws) {
        assertEquals("sample-access-key", aws.getProperties().get("access-key"));
        assertEquals("sample-secret-key", aws.getProperties().get("secret-key"));
        assertEquals("sample-role", aws.getProperties().get("iam-role"));
        assertEquals("sample-region", aws.getProperties().get("region"));
        assertEquals("sample-header", aws.getProperties().get("host-header"));
        assertEquals("sample-group", aws.getProperties().get("security-group-name"));
        assertEquals("sample-tag-key", aws.getProperties().get("tag-key"));
        assertEquals("sample-tag-value", aws.getProperties().get("tag-value"));
        assertEquals("10", aws.getProperties().get("connection-timeout-seconds"));
    }

    protected static void assertPermissionConfig(PermissionConfig expected, Config config) {
        Iterator<PermissionConfig> permConfigs = config.getSecurityConfig().getClientPermissionConfigs().iterator();
        PermissionConfig configured = permConfigs.next();
        assertEquals(expected.getType(), configured.getType());
        assertEquals(expected.getPrincipal(), configured.getPrincipal());
        assertEquals(expected.getName(), configured.getName());
        assertEquals(expected.getActions(), configured.getActions());
    }
}
