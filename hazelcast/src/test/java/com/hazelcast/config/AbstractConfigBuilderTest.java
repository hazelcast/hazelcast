/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.rest.RestConfig;
import com.hazelcast.config.vector.Metric;
import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.config.vector.VectorIndexConfig;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;

import static com.hazelcast.config.RestEndpointGroup.CLUSTER_READ;
import static com.hazelcast.config.RestEndpointGroup.HEALTH_CHECK;
import static com.hazelcast.instance.ProtocolType.CLIENT;
import static com.hazelcast.instance.ProtocolType.MEMCACHE;
import static com.hazelcast.instance.ProtocolType.WAN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Abstract class defining the common test cases for XML and YAML
 * based configuration tests.
 * <p>
 * All common test cases should be defined in this class to guarantee
 * compilation error if either YAML or XML configuration misses to cover
 * a common case.
 * <p>
 * For specific test cases, see {@link XmlOnlyConfigBuilderTest} and
 * {@link YamlOnlyConfigBuilderTest}.
 *
 * @see XMLConfigBuilderTest
 * @see YamlConfigBuilderTest
 */
public abstract class AbstractConfigBuilderTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public abstract void testConfigurationURL() throws Exception;

    @Test
    public abstract void testClusterName();

    @Test
    public abstract void testConfigurationWithFileName() throws Exception;

    @Test(expected = IllegalArgumentException.class)
    public abstract void testConfiguration_withNullInputStream();

    @Test(expected = InvalidConfigurationException.class)
    public abstract void testJoinValidation();

    @Test
    public abstract void testSecurityConfig();

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
    public abstract void readQueueConfig();

    @Test
    public abstract void readListConfig();

    @Test
    public abstract void readSetConfig();

    @Test
    public abstract void readReliableTopicConfig();

    @Test
    public abstract void readTopicConfig();

    @Test
    public abstract void readRingbuffer();

    @Test
    public abstract void testCaseInsensitivityOfSettings();

    @Test
    public abstract void testManagementCenterConfig();

    @Test
    public abstract void testNullManagementCenterConfig();

    @Test
    public abstract void testEmptyManagementCenterConfig();

    @Test
    public abstract void testMapStoreInitialModeLazy();

    @Test
    public abstract void testMapConfig_metadataPolicy();

    @Test
    public abstract void testMapConfig_statisticsEnable();

    @Test
    public abstract void testMapConfig_perEntryStatsEnabled();

    @Test
    public abstract void testMapConfig_metadataPolicy_defaultValue();

    @Test
    public abstract void testMapConfig_evictions();

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
    public abstract void testMapStoreEnabled();

    @Test
    public abstract void testMapStoreEnabledIfNotDisabled();

    @Test
    public abstract void testMapStoreDisabled();

    @Test
    public abstract void testMapStoreConfig_offload_whenDefault();

    @Test
    public abstract void testMapStoreConfig_offload_whenSetFalse();

    @Test
    public abstract void testMapStoreConfig_offload_whenSetTrue();

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
    public abstract void testPartitionGroupNodeAware();

    @Test
    public abstract void testPartitionGroupPlacementAware();

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
    public abstract void testFlakeIdGeneratorConfig();

    @Test(expected = InvalidConfigurationException.class)
    public abstract void testParseExceptionIsNotSwallowed();

    @Test
    public abstract void testMapPartitionLostListenerConfig();

    @Test
    public abstract void testMapPartitionLostListenerConfigReadOnly();

    @Test
    public abstract void testCachePartitionLostListenerConfig();

    @Test
    public abstract void testCachePartitionLostListenerConfigReadOnly();

    @Test
    public abstract void readMulticastConfig();

    @Test
    public abstract void testWanConfig();

    @Test
    public abstract void testSplitBrainProtectionConfig();

    @Test
    public abstract void testSplitBrainProtectionListenerConfig();

    @Test(expected = InvalidConfigurationException.class)
    public abstract void testConfig_whenClassNameAndRecentlyActiveSplitBrainProtectionDefined_exceptionIsThrown();

    @Test(expected = InvalidConfigurationException.class)
    public abstract void testConfig_whenClassNameAndProbabilisticSplitBrainProtectionDefined_exceptionIsThrown();

    @Test(expected = InvalidConfigurationException.class)
    public abstract void testConfig_whenBothBuiltinSplitBrainProtectionsDefined_exceptionIsThrown();

    @Test
    public abstract void testConfig_whenRecentlyActiveSplitBrainProtection_withDefaultValues();

    @Test
    public abstract void testConfig_whenRecentlyActiveSplitBrainProtection_withCustomValues();

    @Test
    public abstract void testConfig_whenProbabilisticSplitBrainProtection_withDefaultValues();

    @Test
    public abstract void testConfig_whenProbabilisticSplitBrainProtection_withCustomValues();

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
    public abstract void testMapCustomEvictionPolicy();

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
    public abstract void testEmptyUserCodeDeployment();

    @Test
    public abstract void testCRDTReplicationConfig();

    @Test
    public abstract void testGlobalSerializer();

    @Test
    public abstract void testJavaSerializationFilter();

    @Test
    public abstract void testJavaReflectionFilter();

    @Test
    public abstract void testCompactSerialization_serializerRegistration();

    @Test
    public abstract void testCompactSerialization_classRegistration();

    @Test
    public abstract void testCompactSerialization_serializerAndClassRegistration();

    @Test
    public abstract void testCompactSerialization_duplicateSerializerRegistration();

    @Test
    public abstract void testCompactSerialization_duplicateClassRegistration();

    @Test
    public abstract void testCompactSerialization_registrationsWithDuplicateClasses();

    @Test
    public abstract void testCompactSerialization_registrationsWithDuplicateTypeNames();

    @Test
    public abstract void testCompactSerialization_withInvalidSerializer();

    @Test
    public abstract void testCompactSerialization_withInvalidCompactSerializableClass();

    @Test
    public abstract void testAllowOverrideDefaultSerializers();

    @Test
    public abstract void testHotRestart();

    @Test
    public abstract void testHotRestartEncryptionAtRest_whenJavaKeyStore();

    @Test
    public abstract void testHotRestartEncryptionAtRest_whenVault();

    @Test
    public abstract void testPersistence();

    @Test
    public abstract void testDynamicConfig();

    @Test
    public abstract void testLocalDevice();

    @Test
    public abstract void testTieredStore();

    @Test
    public abstract void testPersistenceEncryptionAtRest_whenJavaKeyStore();

    @Test
    public abstract void testPersistenceEncryptionAtRest_whenVault();

    @Test
    public abstract void testOnJoinPermissionOperation();

    @Test
    public abstract void testCachePermission();

    @Test
    public abstract void testConfigPermission();

    @Test
    public abstract void testAllPermissionsCovered() throws IOException;

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

    @Test
    public abstract void testDefaultAdvancedNetworkConfig();

    @Test
    public abstract void testAdvancedNetworkConfig_whenInvalidSocketKeepIdleSeconds();

    @Test
    public abstract void testAdvancedNetworkConfig_whenInvalidSocketKeepIntervalSeconds();

    @Test
    public abstract void testAdvancedNetworkConfig_whenInvalidSocketKeepCount();

    @Test
    public abstract void testAmbiguousNetworkConfig_throwsException();

    @Test
    public abstract void testNetworkConfigUnambiguous_whenAdvancedNetworkDisabled();

    @Test
    public abstract void testMultipleClientEndpointConfigs_throwsException();

    @Test
    public abstract void testMultipleRestEndpointConfigs_throwsException();

    @Test
    public abstract void testMultipleMemcacheEndpointConfigs_throwsException();

    @Test
    public abstract void testMultipleJoinElements_throwsException();

    @Test
    public abstract void testMultipleFailureDetectorElements_throwsException();

    @Test
    public abstract void testMultipleMemberAddressProviderElements_throwsException();

    @Test
    public abstract void testMultipleMemberEndpointConfigs_throwsException();

    @Test
    public abstract void testWhitespaceInNonSpaceStrings();

    @Test
    public void testCompleteAdvancedNetworkConfig() {
        Config config = buildCompleteAdvancedNetworkConfig();

        AdvancedNetworkConfig advancedNetworkConfig = config.getAdvancedNetworkConfig();
        JoinConfig joinConfig = advancedNetworkConfig.getJoin();
        IcmpFailureDetectorConfig fdConfig = advancedNetworkConfig.getIcmpFailureDetectorConfig();
        MemberAddressProviderConfig providerConfig = advancedNetworkConfig.getMemberAddressProviderConfig();

        assertTrue(advancedNetworkConfig.isEnabled());
        // join config
        assertFalse(joinConfig.getMulticastConfig().isEnabled());
        assertTrue(joinConfig.getTcpIpConfig().isEnabled());
        assertEquals("10.10.1.10", joinConfig.getTcpIpConfig().getRequiredMember());
        assertContains(joinConfig.getTcpIpConfig().getMembers(), "10.10.1.11");
        assertContains(joinConfig.getTcpIpConfig().getMembers(), "10.10.1.12");
        // failure detector config
        assertTrue(fdConfig.isEnabled());
        assertTrue(fdConfig.isParallelMode());
        assertTrue(fdConfig.isFailFastOnStartup());
        assertEquals(42, fdConfig.getTimeoutMilliseconds());
        assertEquals(42, fdConfig.getMaxAttempts());
        assertEquals(4200, fdConfig.getIntervalMilliseconds());
        assertEquals(255, fdConfig.getTtl());
        // member address provider config
        assertEquals("com.hazelcast.test.Provider", providerConfig.getClassName());

        // endpoint config
        ServerSocketEndpointConfig memberEndpointConfig =
                (ServerSocketEndpointConfig) advancedNetworkConfig.getEndpointConfigs().get(EndpointQualifier.MEMBER);
        assertEquals("member-server-socket", memberEndpointConfig.getName());
        assertEquals(ProtocolType.MEMBER, memberEndpointConfig.getProtocolType());
        // port
        assertEquals(93, memberEndpointConfig.getPortCount());
        assertEquals(9191, memberEndpointConfig.getPort());
        assertFalse(memberEndpointConfig.isPortAutoIncrement());
        // reuse address
        assertTrue(memberEndpointConfig.isReuseAddress());
        // outbound ports
        assertEquals("33000-33100", memberEndpointConfig.getOutboundPortDefinitions().iterator().next());
        // interfaces
        assertTrue(memberEndpointConfig.getInterfaces().isEnabled());
        assertEquals("10.10.0.1", memberEndpointConfig.getInterfaces().getInterfaces().iterator().next());
        // ssl
        assertTrue(memberEndpointConfig.getSSLConfig().isEnabled());
        assertEquals("com.hazelcast.examples.MySSLContextFactory", memberEndpointConfig.getSSLConfig().getFactoryClassName());
        assertEquals("bar", memberEndpointConfig.getSSLConfig().getProperty("foo"));
        // socket interceptor
        assertTrue(memberEndpointConfig.getSocketInterceptorConfig().isEnabled());
        assertEquals("com.hazelcast.examples.MySocketInterceptor",
                memberEndpointConfig.getSocketInterceptorConfig().getClassName());
        assertEquals("baz", memberEndpointConfig.getSocketInterceptorConfig().getProperty("foo"));
        // symmetric encryption config
        assertTrue(memberEndpointConfig.getSymmetricEncryptionConfig().isEnabled());
        assertEquals("Algorithm", memberEndpointConfig.getSymmetricEncryptionConfig().getAlgorithm());
        assertEquals("thesalt", memberEndpointConfig.getSymmetricEncryptionConfig().getSalt());
        assertEquals("thepassword", memberEndpointConfig.getSymmetricEncryptionConfig().getPassword());
        assertEquals(1000, memberEndpointConfig.getSymmetricEncryptionConfig().getIterationCount());
        // socket options
        assertTrue(memberEndpointConfig.isSocketBufferDirect());
        assertTrue(memberEndpointConfig.isSocketTcpNoDelay());
        assertTrue(memberEndpointConfig.isSocketKeepAlive());
        assertEquals(33, memberEndpointConfig.getSocketConnectTimeoutSeconds());
        assertEquals(34, memberEndpointConfig.getSocketSendBufferSizeKb());
        assertEquals(67, memberEndpointConfig.getSocketRcvBufferSizeKb());
        assertEquals(11, memberEndpointConfig.getSocketLingerSeconds());
        assertEquals(12, memberEndpointConfig.getSocketKeepCount());
        assertEquals(13, memberEndpointConfig.getSocketKeepIntervalSeconds());
        assertEquals(14, memberEndpointConfig.getSocketKeepIdleSeconds());

        RestServerEndpointConfig restServerEndpointConfig = advancedNetworkConfig.getRestEndpointConfig();
        assertEquals(8080, restServerEndpointConfig.getPort());
        assertContainsAll(restServerEndpointConfig.getEnabledGroups(),
                Arrays.asList(CLUSTER_READ, RestEndpointGroup.WAN, HEALTH_CHECK));

        // memcache config
        EndpointConfig memcacheEndpointConfig = advancedNetworkConfig.getEndpointConfigs().get(EndpointQualifier.MEMCACHE);
        assertEquals(MEMCACHE, memcacheEndpointConfig.getProtocolType());
        assertEquals("42000-42100", memcacheEndpointConfig.getOutboundPortDefinitions().iterator().next());

        // WAN server socket configs
        EndpointConfig wanServerSockerEndpointConfig1 = advancedNetworkConfig.getEndpointConfigs()
                .get(EndpointQualifier.resolve(WAN, "WAN_SERVER1"));
        EndpointConfig wanServerSockerEndpointConfig2 = advancedNetworkConfig.getEndpointConfigs()
                .get(EndpointQualifier.resolve(WAN, "WAN_SERVER2"));
        assertEquals(WAN, wanServerSockerEndpointConfig1.getProtocolType());
        assertEquals("52000-52100", wanServerSockerEndpointConfig1.getOutboundPortDefinitions().iterator().next());
        assertEquals(WAN, wanServerSockerEndpointConfig2.getProtocolType());
        assertEquals("53000-53100", wanServerSockerEndpointConfig2.getOutboundPortDefinitions().iterator().next());

        // WAN endpoint config
        EndpointConfig wanEndpointConfig1 = advancedNetworkConfig.getEndpointConfigs()
                .get(EndpointQualifier.resolve(WAN, "WAN_ENDPOINT1"));
        EndpointConfig wanEndpointConfig2 = advancedNetworkConfig.getEndpointConfigs()
                .get(EndpointQualifier.resolve(WAN, "WAN_ENDPOINT2"));
        assertEquals(WAN, wanEndpointConfig1.getProtocolType());
        assertEquals("62000-62100", wanEndpointConfig1.getOutboundPortDefinitions().iterator().next());
        assertEquals(WAN, wanEndpointConfig2.getProtocolType());
        assertEquals("63000-63100", wanEndpointConfig2.getOutboundPortDefinitions().iterator().next());

        // client server socket config
        EndpointConfig clientServerSocketConfig = advancedNetworkConfig.getEndpointConfigs().get(EndpointQualifier.CLIENT);
        assertEquals(CLIENT, clientServerSocketConfig.getProtocolType());
        assertEquals("72000-72100", clientServerSocketConfig.getOutboundPortDefinitions().iterator().next());
    }

    protected abstract Config buildCompleteAdvancedNetworkConfig();

    public abstract void testCPSubsystemConfig();

    public abstract void testMetricsConfig();

    public abstract void testInstanceTrackingConfig();

    public abstract void testMetricsConfigMasterSwitchDisabled();

    public abstract void testMetricsConfigMcDisabled();

    public abstract void testMetricsConfigJmxDisabled();

    @Test
    public void testAuditlogConfig() {
        Config config = buildAuditlogConfig();
        AuditlogConfig auditlogConfig = config.getAuditlogConfig();
        assertNotNull(auditlogConfig);
        assertTrue(auditlogConfig.isEnabled());

        assertEquals("com.acme.auditlog.AuditlogToSyslogFactory", auditlogConfig.getFactoryClassName());
        Properties properties = auditlogConfig.getProperties();
        assertNotNull(properties);
        assertEquals("syslogserver.acme.com", properties.get("host"));
        assertEquals("514", properties.get("port"));
    }

    public abstract void testSqlConfig();

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
        assertEquals(expected.isDeny(), configured.isDeny());
    }

    @Test
    public abstract void testPersistentMemoryDirectoryConfiguration() throws IOException;

    @Test
    public abstract void testPersistentMemoryDirectoryConfigurationSimple();

    @Test(expected = InvalidConfigurationException.class)
    public abstract void testPersistentMemoryDirectoryConfiguration_uniqueDirViolationThrows();

    @Test(expected = InvalidConfigurationException.class)
    public abstract void testPersistentMemoryDirectoryConfiguration_uniqueNumaNodeViolationThrows();

    @Test(expected = InvalidConfigurationException.class)
    public abstract void testPersistentMemoryDirectoryConfiguration_numaNodeConsistencyViolationThrows();

    @Test
    public abstract void testPersistentMemoryDirectoryConfiguration_simpleAndAdvancedPasses();

    @Test
    public abstract void testPersistentMemoryConfiguration_SystemMemoryMode();

    @Test(expected = InvalidConfigurationException.class)
    public abstract void testPersistentMemoryConfiguration_NotExistingModeThrows();

    @Test(expected = InvalidConfigurationException.class)
    public abstract void testPersistentMemoryDirectoryConfiguration_SystemMemoryModeThrows();

    @Test
    public void testMapWildcardConfig() {
        Config config = buildMapWildcardConfig();

        MapConfig map1 = config.getMapConfig("mapA");
        assertEquals(1, map1.getBackupCount());
        assertEquals(1, map1.getAttributeConfigs().size());

        MapConfig mapWith2Backups = config.getMapConfig("mapBackup2A");
        assertEquals(2, mapWith2Backups.getBackupCount());
        assertEquals(1, map1.getAttributeConfigs().size());
    }

    static void validateRestConfig(Config config) {
        RestConfig restConfig = config.getRestConfig();
        assertTrue(restConfig.isEnabled());
        assertEquals(8080, restConfig.getPort());
        assertEquals("realmName", restConfig.getSecurityRealm());
        assertEquals(500, restConfig.getTokenValidityDuration().toSeconds());
        assertEquals(10, restConfig.getMaxLoginAttempts());
        assertEquals(10, restConfig.getLockoutDuration().getSeconds());
        assertTrue(restConfig.getSsl().isEnabled());
        assertEquals(RestConfig.Ssl.ClientAuth.NEED, restConfig.getSsl().getClientAuth());
        assertEquals("TLS_RSA_WITH_AES_128_CBC_SHA, TLS_RSA_WITH_AES_128_CBC_SHA256", restConfig.getSsl().getCiphers());
        assertEquals("TLSv1.2, TLSv1.3", restConfig.getSsl().getEnabledProtocols());
        assertEquals("myKeyAlias", restConfig.getSsl().getKeyAlias());
        assertEquals("myKeyPassword", restConfig.getSsl().getKeyPassword());
        assertEquals("/path/to/keystore", restConfig.getSsl().getKeyStore());
        assertEquals("myKeyStorePassword", restConfig.getSsl().getKeyStorePassword());
        assertEquals("JKS", restConfig.getSsl().getKeyStoreType());
        assertEquals("SUN", restConfig.getSsl().getKeyStoreProvider());
        assertEquals("/path/to/truststore", restConfig.getSsl().getTrustStore());
        assertEquals("myTrustStorePassword", restConfig.getSsl().getTrustStorePassword());
        assertEquals("JKS", restConfig.getSsl().getTrustStoreType());
        assertEquals("SUN", restConfig.getSsl().getTrustStoreProvider());
        assertEquals("TLS", restConfig.getSsl().getProtocol());
        assertEquals("/path/to/certificate", restConfig.getSsl().getCertificate());
        assertEquals("/path/to/certificate-key", restConfig.getSsl().getCertificatePrivateKey());
        assertEquals("/path/to/trust-certificate", restConfig.getSsl().getTrustCertificate());
        assertEquals("/path/to/trust-certificate-key", restConfig.getSsl().getTrustCertificatePrivateKey());
    }

    @Test
    public abstract void testMapExpiryConfig();

    @Test
    public abstract void testIntegrityCheckerConfig();

    @Test
    public abstract void testDataConnectionConfigs();

    @Test
    public abstract void testTpcConfig();

    @Test
    public abstract void testTpcSocketConfig();

    @Test
    public abstract void testTpcSocketConfigAdvanced();

    @Test
    public abstract void testPartitioningAttributeConfigs();

    @Test
    public abstract void testNamespaceConfigs() throws IOException;

    @Test
    public abstract void testRestConfig() throws IOException;

    @Test
    public abstract void testVectorCollectionConfig();

    @Test
    public abstract void testVectorCollectionConfig_backupCount_max();

    @Test(expected = InvalidConfigurationException.class)
    public abstract void testVectorCollectionConfig_backupCount_moreThanMax();

    @Test
    public abstract void testVectorCollectionConfig_backupCount_min();

    @Test(expected = InvalidConfigurationException.class)
    public abstract void testVectorCollectionConfig_backupCount_lessThanMin();

    @Test
    public abstract void testVectorCollectionConfig_asyncBackupCount_max();

    @Test(expected = InvalidConfigurationException.class)
    public abstract void testVectorCollectionConfig_asyncBackupCount_moreThanMax();

    @Test
    public abstract void testVectorCollectionConfig_asyncBackupCount_min();

    @Test(expected = InvalidConfigurationException.class)
    public abstract void testVectorCollectionConfig_asyncBackupCount_lessThanMin();

    @Test
    public abstract void testVectorCollectionConfig_backupSyncAndAsyncCount_max();

    @Test(expected = InvalidConfigurationException.class)
    public abstract void testVectorCollectionConfig_backupSyncAndAsyncCount_moreThanMax();

    @Test(expected = InvalidConfigurationException.class)
    public abstract void testVectorCollectionConfig_multipleIndexesWithTheSameName_fail();

    protected void validateVectorCollectionConfig(Config config) {
        var vectorCollectionConfigs = config.getVectorCollectionConfigs();
        var expectedCollectionConfigs = new HashMap<String, VectorCollectionConfig>();
        var expectedMergePolicyConfig = new MergePolicyConfig("CustomMergePolicy", 132);
        expectedCollectionConfigs.put(
                "vector-1",
                new VectorCollectionConfig("vector-1")
                        .addVectorIndexConfig(
                                new VectorIndexConfig()
                                        .setName("index-1-1")
                                        .setDimension(2)
                                        .setMetric(Metric.DOT)
                                        .setMaxDegree(10)
                                        .setEfConstruction(10)
                                        .setUseDeduplication(true)
                        )
                        .addVectorIndexConfig(
                                new VectorIndexConfig()
                                        .setName("index-1-2")
                                        .setDimension(3)
                                        .setMetric(Metric.EUCLIDEAN)
                        )
        );
        expectedCollectionConfigs.put(
                "vector-2",
                new VectorCollectionConfig("vector-2")
                        .setBackupCount(2)
                        .setAsyncBackupCount(1)
                        .setSplitBrainProtectionName("splitBrainProtectionName")
                        .setMergePolicyConfig(expectedMergePolicyConfig)
                        .setUserCodeNamespace("ns1")
                        .addVectorIndexConfig(
                                new VectorIndexConfig()
                                        .setDimension(4)
                                        .setMetric(Metric.COSINE)
                                        .setUseDeduplication(false)
                        )
        );
        assertThat(vectorCollectionConfigs).usingRecursiveComparison().isEqualTo(expectedCollectionConfigs);
    }

    protected abstract Config buildAuditlogConfig();

    /**
     * Build a config with overlapping wildcard configs map* & mapBackup2*
     */
    protected abstract Config buildMapWildcardConfig();

}
