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

package com.hazelcast.config;

import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.DurationConfig;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig;
import com.hazelcast.config.ConfigCompatibilityChecker.EventJournalConfigChecker;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.spi.merge.HigherHitsMergePolicy;
import com.hazelcast.spi.merge.LatestUpdateMergePolicy;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.TopicOverloadPolicy;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;

import static com.google.common.collect.Sets.newHashSet;
import static com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig.ExpiryPolicyType.ACCESSED;
import static com.hazelcast.config.ConfigXmlGenerator.MASK_FOR_SENSITIVE_DATA;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ConfigXmlGeneratorTest {

    @Test
    public void testIfSensitiveDataIsMasked_whenMaskingEnabled() {
        Config cfg = new Config();
        SSLConfig sslConfig = new SSLConfig();
        sslConfig.setProperty("keyStorePassword", "Hazelcast")
                .setProperty("trustStorePassword", "Hazelcast");
        cfg.getNetworkConfig().setSSLConfig(sslConfig);

        SymmetricEncryptionConfig symmetricEncryptionConfig = new SymmetricEncryptionConfig();
        symmetricEncryptionConfig.setPassword("Hazelcast");
        symmetricEncryptionConfig.setSalt("theSalt");

        cfg.getNetworkConfig().setSymmetricEncryptionConfig(symmetricEncryptionConfig);
        cfg.setLicenseKey("HazelcastLicenseKey");

        Config newConfigViaXMLGenerator = getNewConfigViaXMLGenerator(cfg);
        SSLConfig generatedSSLConfig = newConfigViaXMLGenerator.getNetworkConfig().getSSLConfig();

        assertEquals(generatedSSLConfig.getProperty("keyStorePassword"), MASK_FOR_SENSITIVE_DATA);
        assertEquals(generatedSSLConfig.getProperty("trustStorePassword"), MASK_FOR_SENSITIVE_DATA);

        String secPassword = newConfigViaXMLGenerator.getNetworkConfig().getSymmetricEncryptionConfig().getPassword();
        String theSalt = newConfigViaXMLGenerator.getNetworkConfig().getSymmetricEncryptionConfig().getSalt();
        assertEquals(secPassword, MASK_FOR_SENSITIVE_DATA);
        assertEquals(theSalt, MASK_FOR_SENSITIVE_DATA);
        assertEquals(newConfigViaXMLGenerator.getLicenseKey(), MASK_FOR_SENSITIVE_DATA);
        assertEquals(newConfigViaXMLGenerator.getGroupConfig().getPassword(), MASK_FOR_SENSITIVE_DATA);
    }

    @Test
    public void testIfSensitiveDataIsNotMasked_whenMaskingDisabled() {
        String password = "Hazelcast";
        String salt = "theSalt";
        String licenseKey = "HazelcastLicenseKey";

        Config cfg = new Config();
        cfg.getGroupConfig().setPassword(password);

        SSLConfig sslConfig = new SSLConfig();
        sslConfig.setProperty("keyStorePassword", password)
                .setProperty("trustStorePassword", password);
        cfg.getNetworkConfig().setSSLConfig(sslConfig);

        SymmetricEncryptionConfig symmetricEncryptionConfig = new SymmetricEncryptionConfig();
        symmetricEncryptionConfig.setPassword(password);
        symmetricEncryptionConfig.setSalt(salt);

        cfg.getNetworkConfig().setSymmetricEncryptionConfig(symmetricEncryptionConfig);
        cfg.setLicenseKey(licenseKey);

        Config newConfigViaXMLGenerator = getNewConfigViaXMLGenerator(cfg, false);
        SSLConfig generatedSSLConfig = newConfigViaXMLGenerator.getNetworkConfig().getSSLConfig();

        assertEquals(generatedSSLConfig.getProperty("keyStorePassword"), password);
        assertEquals(generatedSSLConfig.getProperty("trustStorePassword"), password);

        String secPassword = newConfigViaXMLGenerator.getNetworkConfig().getSymmetricEncryptionConfig().getPassword();
        String theSalt = newConfigViaXMLGenerator.getNetworkConfig().getSymmetricEncryptionConfig().getSalt();
        assertEquals(secPassword, password);
        assertEquals(theSalt, salt);
        assertEquals(newConfigViaXMLGenerator.getLicenseKey(), licenseKey);
        assertEquals(newConfigViaXMLGenerator.getGroupConfig().getPassword(), password);
    }

    @Test
    public void testMemberAddressProvider() {
        Config cfg = new Config();
        MemberAddressProviderConfig expected = cfg.getNetworkConfig().getMemberAddressProviderConfig();
        expected.setEnabled(true)
                .setEnabled(true)
                .setClassName("ClassName");
        Properties props = expected.getProperties();
        props.setProperty("p1", "v1");
        props.setProperty("p2", "v2");
        props.setProperty("p3", "v3");

        Config newConfigViaXMLGenerator = getNewConfigViaXMLGenerator(cfg);
        MemberAddressProviderConfig actual = newConfigViaXMLGenerator.getNetworkConfig().getMemberAddressProviderConfig();

        assertEquals(expected.isEnabled(), actual.isEnabled());
        assertEquals(expected.getClassName(), actual.getClassName());
        assertEquals(expected.getProperties(), actual.getProperties());
        assertEquals(expected, actual);
    }

    @Test
    public void testFailureDetectorConfigGenerator() {
        Config cfg = new Config();
        IcmpFailureDetectorConfig expected = new IcmpFailureDetectorConfig();
        expected.setEnabled(true)
                .setIntervalMilliseconds(1001)
                .setTimeoutMilliseconds(1002)
                .setMaxAttempts(4)
                .setTtl(300)
                .setParallelMode(false) // Defaults to false
                .setFailFastOnStartup(false); // Defaults to false

        cfg.getNetworkConfig().setIcmpFailureDetectorConfig(expected);

        Config newConfigViaXMLGenerator = getNewConfigViaXMLGenerator(cfg);
        IcmpFailureDetectorConfig actual = newConfigViaXMLGenerator.getNetworkConfig().getIcmpFailureDetectorConfig();

        assertEquals(expected.isEnabled(), actual.isEnabled());
        assertEquals(expected.getIntervalMilliseconds(), actual.getIntervalMilliseconds());
        assertEquals(expected.getTimeoutMilliseconds(), actual.getTimeoutMilliseconds());
        assertEquals(expected.getTtl(), actual.getTtl());
        assertEquals(expected.getMaxAttempts(), actual.getMaxAttempts());
        assertEquals(expected.isFailFastOnStartup(), actual.isFailFastOnStartup());
        assertEquals(expected.isParallelMode(), actual.isParallelMode());
        assertEquals(expected, actual);
    }

    @Test
    public void testNetworkMulticastJoinConfig() {
        Config cfg = new Config();

        MulticastConfig expectedConfig = new MulticastConfig()
                .setEnabled(true)
                .setMulticastTimeoutSeconds(10)
                .setLoopbackModeEnabled(true)
                .setMulticastGroup("224.2.2.3")
                .setMulticastTimeToLive(42)
                .setMulticastPort(4242)
                .setTrustedInterfaces(newHashSet("*"));

        cfg.getNetworkConfig().getJoin().setMulticastConfig(expectedConfig);

        MulticastConfig actualConfig = getNewConfigViaXMLGenerator(cfg).getNetworkConfig().getJoin().getMulticastConfig();

        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testNetworkTcpJoinConfig() {
        Config cfg = new Config();

        TcpIpConfig expectedConfig = new TcpIpConfig()
                .setEnabled(true)
                .setConnectionTimeoutSeconds(10)
                .addMember("10.11.12.1,10.11.12.2")
                .setRequiredMember("10.11.11.2");

        cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        cfg.getNetworkConfig().getJoin().setTcpIpConfig(expectedConfig);

        TcpIpConfig actualConfig = getNewConfigViaXMLGenerator(cfg).getNetworkConfig().getJoin().getTcpIpConfig();

        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testNetworkConfigOutboundPorts() {
        Config cfg = new Config();

        NetworkConfig expectedNetworkConfig = cfg.getNetworkConfig();
        expectedNetworkConfig
                .addOutboundPortDefinition("4242-4244")
                .addOutboundPortDefinition("5252;5254");

        NetworkConfig actualNetworkConfig = getNewConfigViaXMLGenerator(cfg).getNetworkConfig();

        assertEquals(expectedNetworkConfig.getOutboundPortDefinitions(), actualNetworkConfig.getOutboundPortDefinitions());
        assertEquals(expectedNetworkConfig.getOutboundPorts(), actualNetworkConfig.getOutboundPorts());
    }

    @Test
    public void testNetworkConfigInterfaces() {
        Config cfg = new Config();

        NetworkConfig expectedNetworkConfig = cfg.getNetworkConfig();
        expectedNetworkConfig.getInterfaces()
                .addInterface("127.0.0.*")
                .setEnabled(true);

        NetworkConfig actualNetworkConfig = getNewConfigViaXMLGenerator(cfg).getNetworkConfig();

        assertEquals(expectedNetworkConfig.getInterfaces(), actualNetworkConfig.getInterfaces());
    }

    @Test
    public void testNetworkConfigSocketInterceptor() {
        Config cfg = new Config();

        SocketInterceptorConfig expectedConfig = new SocketInterceptorConfig()
                .setEnabled(true)
                .setClassName("socketInterceptor")
                .setProperty("key", "value");

        cfg.getNetworkConfig().setSocketInterceptorConfig(expectedConfig);

        SocketInterceptorConfig actualConfig = getNewConfigViaXMLGenerator(cfg).getNetworkConfig().getSocketInterceptorConfig();

        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testListenerConfig() {
        Config expectedConfig = new Config();

        expectedConfig.setListenerConfigs(asList(new ListenerConfig("Listener")));

        Config actualConfig = getNewConfigViaXMLGenerator(expectedConfig);

        assertEquals(expectedConfig.getListenerConfigs(), actualConfig.getListenerConfigs());
    }

    @Test
    public void testHotRestartPersistenceConfig() {
        Config cfg = new Config();

        HotRestartPersistenceConfig expectedConfig = cfg.getHotRestartPersistenceConfig();
        expectedConfig.setEnabled(true)
                .setClusterDataRecoveryPolicy(HotRestartClusterDataRecoveryPolicy.FULL_RECOVERY_ONLY)
                .setValidationTimeoutSeconds(100)
                .setDataLoadTimeoutSeconds(130)
                .setBaseDir(new File("/nonexisting-base"))
                .setBackupDir(new File("/nonexisting-backup"))
                .setParallelism(5);

        HotRestartPersistenceConfig actualConfig = getNewConfigViaXMLGenerator(cfg).getHotRestartPersistenceConfig();

        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testServicesConfig() {
        Config cfg = new Config();

        Properties properties = new Properties();
        properties.setProperty("key", "value");

        ServiceConfig serviceConfig = new ServiceConfig()
                .setName("ServiceConfig")
                .setEnabled(true)
                .setClassName("ServiceClass")
                .setProperties(properties);
        ServicesConfig expectedConfig = cfg.getServicesConfig()
                .setEnableDefaults(true)
                .setServiceConfigs(asList(serviceConfig));

        ServicesConfig actualConfig = getNewConfigViaXMLGenerator(cfg).getServicesConfig();

        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testSecurityConfig() {
        Config cfg = new Config();

        Properties dummyprops = new Properties();
        dummyprops.put("a", "b");

        SecurityConfig expectedConfig = new SecurityConfig();
        expectedConfig.setEnabled(true)
          .setClientBlockUnmappedActions(false)
          .setClientLoginModuleConfigs(Arrays.asList(
                  new LoginModuleConfig()
                          .setClassName("f.o.o")
                          .setUsage(LoginModuleConfig.LoginModuleUsage.OPTIONAL),
                  new LoginModuleConfig()
                          .setClassName("b.a.r")
                          .setUsage(LoginModuleConfig.LoginModuleUsage.SUFFICIENT),
                  new LoginModuleConfig()
                          .setClassName("l.o.l")
                          .setUsage(LoginModuleConfig.LoginModuleUsage.REQUIRED)))
          .setMemberLoginModuleConfigs(Arrays.asList(
                  new LoginModuleConfig()
                          .setClassName("member.f.o.o")
                          .setUsage(LoginModuleConfig.LoginModuleUsage.OPTIONAL),
                  new LoginModuleConfig()
                          .setClassName("member.b.a.r")
                          .setUsage(LoginModuleConfig.LoginModuleUsage.SUFFICIENT),
                  new LoginModuleConfig()
                          .setClassName("member.l.o.l")
                          .setUsage(LoginModuleConfig.LoginModuleUsage.REQUIRED)))
        .setMemberCredentialsConfig(new CredentialsFactoryConfig().setClassName("foo.bar").setProperties(dummyprops))
        .setClientPermissionConfigs(new HashSet<PermissionConfig>(Arrays.asList(
                new PermissionConfig()
                        .setActions(newHashSet("read", "remove"))
                        .setEndpoints(newHashSet("127.0.0.1", "127.0.0.2"))
                        .setType(PermissionConfig.PermissionType.ATOMIC_LONG)
                        .setName("mycounter")
                        .setPrincipal("devos"))));

        cfg.setSecurityConfig(expectedConfig);

        SecurityConfig actualConfig = getNewConfigViaXMLGenerator(cfg).getSecurityConfig();
        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testSerializationConfig() {
        Config cfg = new Config();

        GlobalSerializerConfig globalSerializerConfig = new GlobalSerializerConfig()
                .setClassName("GlobalSerializer")
                .setOverrideJavaSerialization(true);

        SerializerConfig serializerConfig = new SerializerConfig()
                .setClassName("SerializerClass")
                .setTypeClassName("TypeClass");

        SerializationConfig expectedConfig = new SerializationConfig()
                .setAllowUnsafe(true)
                .setPortableVersion(2)
                .setByteOrder(ByteOrder.BIG_ENDIAN)
                .setUseNativeByteOrder(true)
                .setCheckClassDefErrors(true)
                .setEnableCompression(true)
                .setEnableSharedObject(true)
                .setGlobalSerializerConfig(globalSerializerConfig)
                .addDataSerializableFactoryClass(10, "SerializableFactory")
                .addPortableFactoryClass(10, "PortableFactory")
                .addSerializerConfig(serializerConfig);

        cfg.setSerializationConfig(expectedConfig);

        SerializationConfig actualConfig = getNewConfigViaXMLGenerator(cfg).getSerializationConfig();

        assertEquals(expectedConfig.isAllowUnsafe(), actualConfig.isAllowUnsafe());
        assertEquals(expectedConfig.getPortableVersion(), actualConfig.getPortableVersion());
        assertEquals(expectedConfig.getByteOrder(), actualConfig.getByteOrder());
        assertEquals(expectedConfig.isUseNativeByteOrder(), actualConfig.isUseNativeByteOrder());
        assertEquals(expectedConfig.isCheckClassDefErrors(), actualConfig.isCheckClassDefErrors());
        assertEquals(expectedConfig.isEnableCompression(), actualConfig.isEnableCompression());
        assertEquals(expectedConfig.isEnableSharedObject(), actualConfig.isEnableSharedObject());
        assertEquals(expectedConfig.getGlobalSerializerConfig(), actualConfig.getGlobalSerializerConfig());
        assertEquals(expectedConfig.getDataSerializableFactoryClasses(), actualConfig.getDataSerializableFactoryClasses());
        assertEquals(expectedConfig.getPortableFactoryClasses(), actualConfig.getPortableFactoryClasses());
        assertEquals(expectedConfig.getSerializerConfigs(), actualConfig.getSerializerConfigs());
    }

    @Test
    public void testPartitionGroupConfig() {
        Config cfg = new Config();

        PartitionGroupConfig expectedConfig = new PartitionGroupConfig()
                .setEnabled(true)
                .setGroupType(PartitionGroupConfig.MemberGroupType.PER_MEMBER)
                .setMemberGroupConfigs(asList(new MemberGroupConfig().addInterface("hostname")));

        cfg.setPartitionGroupConfig(expectedConfig);

        PartitionGroupConfig actualConfig = getNewConfigViaXMLGenerator(cfg).getPartitionGroupConfig();

        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testManagementCenterConfigGenerator() {
        ManagementCenterConfig managementCenterConfig = new ManagementCenterConfig()
                .setEnabled(true)
                .setUpdateInterval(8)
                .setUrl("http://foomybar.ber")
                .setMutualAuthConfig(
                        new MCMutualAuthConfig()
                                .setEnabled(true)
                                .setProperty("keyStore", "/tmp/foo_keystore")
                                .setProperty("keyStorePassword", "myp@ss1")
                                .setProperty("trustStore", "/tmp/foo_truststore")
                                .setProperty("trustStorePassword", "myp@ss2")
                );

        Config config = new Config()
                .setManagementCenterConfig(managementCenterConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        ManagementCenterConfig xmlManCenterConfig = xmlConfig.getManagementCenterConfig();
        assertEquals(managementCenterConfig.isEnabled(), xmlManCenterConfig.isEnabled());
        assertEquals(managementCenterConfig.getUpdateInterval(), xmlManCenterConfig.getUpdateInterval());
        assertEquals(managementCenterConfig.getUrl(), xmlManCenterConfig.getUrl());
        assertEquals(managementCenterConfig.getMutualAuthConfig().isEnabled(), xmlManCenterConfig.getMutualAuthConfig().isEnabled());
        assertEquals(managementCenterConfig.getMutualAuthConfig().getFactoryClassName(), xmlManCenterConfig.getMutualAuthConfig().getFactoryClassName());
        assertEquals(managementCenterConfig.getMutualAuthConfig().getProperty("keyStore"), xmlManCenterConfig.getMutualAuthConfig().getProperty("keyStore"));
        assertEquals(managementCenterConfig.getMutualAuthConfig().getProperty("trustStore"), xmlManCenterConfig.getMutualAuthConfig().getProperty("trustStore"));
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testReplicatedMapConfigGenerator() {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig()
                .setPolicy("PassThroughMergePolicy")
                .setBatchSize(1234);

        ReplicatedMapConfig replicatedMapConfig = new ReplicatedMapConfig()
                .setName("replicated-map-name")
                .setStatisticsEnabled(false)
                .setConcurrencyLevel(128)
                .setQuorumName("quorum")
                .setMergePolicyConfig(mergePolicyConfig)
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .addEntryListenerConfig(new EntryListenerConfig("com.hazelcast.entrylistener", false, false));

        replicatedMapConfig.setAsyncFillup(true);

        Config config = new Config()
                .addReplicatedMapConfig(replicatedMapConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        ReplicatedMapConfig xmlReplicatedMapConfig = xmlConfig.getReplicatedMapConfig("replicated-map-name");
        MergePolicyConfig actualMergePolicyConfig = xmlReplicatedMapConfig.getMergePolicyConfig();
        assertEquals("replicated-map-name", xmlReplicatedMapConfig.getName());
        assertFalse(xmlReplicatedMapConfig.isStatisticsEnabled());
        assertEquals(128, xmlReplicatedMapConfig.getConcurrencyLevel());
        assertEquals("com.hazelcast.entrylistener", xmlReplicatedMapConfig.getListenerConfigs().get(0).getClassName());
        assertEquals("quorum", xmlReplicatedMapConfig.getQuorumName());
        assertEquals(InMemoryFormat.NATIVE, xmlReplicatedMapConfig.getInMemoryFormat());
        assertTrue(xmlReplicatedMapConfig.isAsyncFillup());
        assertEquals("PassThroughMergePolicy", actualMergePolicyConfig.getPolicy());
        assertEquals(1234, actualMergePolicyConfig.getBatchSize());
        assertEquals(replicatedMapConfig, xmlReplicatedMapConfig);
    }

    @Test
    public void testFlakeIdGeneratorConfigGenerator() {
        FlakeIdGeneratorConfig figConfig = new FlakeIdGeneratorConfig("flake-id-gen1")
                .setPrefetchCount(3)
                .setPrefetchValidityMillis(10L)
                .setIdOffset(20L)
                .setNodeIdOffset(30L)
                .setStatisticsEnabled(false);

        Config config = new Config()
                .addFlakeIdGeneratorConfig(figConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        FlakeIdGeneratorConfig xmlReplicatedConfig = xmlConfig.getFlakeIdGeneratorConfig("flake-id-gen1");
        assertEquals(figConfig, xmlReplicatedConfig);
    }

    @Test
    public void testCacheAttributes() {
        CacheSimpleConfig expectedConfig = new CacheSimpleConfig()
                .setName("testCache")
                .setEvictionConfig(evictionConfig())
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setBackupCount(2)
                .setAsyncBackupCount(3)
                .setCacheLoader("cacheLoader")
                .setCacheWriter("cacheWriter")
                .setExpiryPolicyFactoryConfig(new ExpiryPolicyFactoryConfig("expiryPolicyFactory"))
                .setManagementEnabled(true)
                .setStatisticsEnabled(true)
                .setKeyType("keyType")
                .setValueType("valueType")
                .setReadThrough(true)
                .setHotRestartConfig(hotRestartConfig())
                .setCacheEntryListeners(asList(cacheSimpleEntryListenerConfig()))
                .setWriteThrough(true)
                .setPartitionLostListenerConfigs(asList(new CachePartitionLostListenerConfig("partitionLostListener")))
                .setQuorumName("testQuorum");

        expectedConfig.setMergePolicy("mergePolicy");
        expectedConfig.setDisablePerEntryInvalidationEvents(true);
        expectedConfig.setWanReplicationRef(wanReplicationRef());

        Config config = new Config()
                .addCacheConfig(expectedConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        CacheSimpleConfig actualConfig = xmlConfig.getCacheConfig("testCache");
        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testCacheFactoryAttributes() {
        TimedExpiryPolicyFactoryConfig timedExpiryPolicyFactoryConfig = new TimedExpiryPolicyFactoryConfig(ACCESSED,
                new DurationConfig(10, SECONDS));

        CacheSimpleConfig expectedConfig = new CacheSimpleConfig()
                .setName("testCache")
                .setCacheLoaderFactory("cacheLoaderFactory")
                .setCacheWriterFactory("cacheWriterFactory")
                .setExpiryPolicyFactory("expiryPolicyFactory")
                .setCacheEntryListeners(asList(cacheSimpleEntryListenerConfig()))
                .setExpiryPolicyFactoryConfig(new ExpiryPolicyFactoryConfig(timedExpiryPolicyFactoryConfig))
                .setPartitionLostListenerConfigs(asList(new CachePartitionLostListenerConfig("partitionLostListener")));

        expectedConfig.setMergePolicy("mergePolicy");
        expectedConfig.setDisablePerEntryInvalidationEvents(true);

        Config config = new Config()
                .addCacheConfig(expectedConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        CacheSimpleConfig actualConfig = xmlConfig.getCacheConfig("testCache");
        assertEquals(expectedConfig, actualConfig);
    }

    private static CacheSimpleEntryListenerConfig cacheSimpleEntryListenerConfig() {
        CacheSimpleEntryListenerConfig entryListenerConfig = new CacheSimpleEntryListenerConfig();
        entryListenerConfig.setCacheEntryListenerFactory("entryListenerFactory");
        entryListenerConfig.setSynchronous(true);
        entryListenerConfig.setOldValueRequired(true);
        entryListenerConfig.setCacheEntryEventFilterFactory("entryEventFilterFactory");
        return entryListenerConfig;
    }

    @Test
    public void testCacheQuorumRef() {
        CacheSimpleConfig expectedConfig = new CacheSimpleConfig()
                .setName("testCache")
                .setQuorumName("testQuorum");

        Config config = new Config()
                .addCacheConfig(expectedConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        CacheSimpleConfig actualConfig = xmlConfig.getCacheConfig("testCache");
        assertEquals("testQuorum", actualConfig.getQuorumName());
    }

    @Test
    public void testRingbufferWithStoreClass() {
        RingbufferStoreConfig ringbufferStoreConfig = new RingbufferStoreConfig()
                .setEnabled(true)
                .setClassName("ClassName")
                .setProperty("p1", "v1")
                .setProperty("p2", "v2")
                .setProperty("p3", "v3");

        testRingbuffer(ringbufferStoreConfig);
    }

    @Test
    public void testRingbufferWithStoreFactory() {
        RingbufferStoreConfig ringbufferStoreConfig = new RingbufferStoreConfig()
                .setEnabled(true)
                .setFactoryClassName("FactoryClassName")
                .setProperty("p1", "v1")
                .setProperty("p2", "v2")
                .setProperty("p3", "v3");

        testRingbuffer(ringbufferStoreConfig);
    }

    private void testRingbuffer(RingbufferStoreConfig ringbufferStoreConfig) {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig()
                .setPolicy("PassThroughMergePolicy")
                .setBatchSize(1234);
        RingbufferConfig expectedConfig = new RingbufferConfig("testRbConfig")
                .setBackupCount(1)
                .setAsyncBackupCount(2)
                .setCapacity(3)
                .setTimeToLiveSeconds(4)
                .setInMemoryFormat(InMemoryFormat.BINARY)
                .setRingbufferStoreConfig(ringbufferStoreConfig)
                .setQuorumName("quorum")
                .setMergePolicyConfig(mergePolicyConfig);

        Config config = new Config().addRingBufferConfig(expectedConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        RingbufferConfig actualConfig = xmlConfig.getRingbufferConfig(expectedConfig.getName());
        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testSemaphore() {
        SemaphoreConfig expectedConfig = new SemaphoreConfig()
                .setName("testSemaphore")
                .setQuorumName("quorum")
                .setInitialPermits(3)
                .setBackupCount(1)
                .setAsyncBackupCount(2);

        Config config = new Config()
                .addSemaphoreConfig(expectedConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        SemaphoreConfig actualConfig = xmlConfig.getSemaphoreConfig(expectedConfig.getName());
        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testExecutor() {
        ExecutorConfig expectedConfig = new ExecutorConfig()
                .setName("testExecutor")
                .setStatisticsEnabled(true)
                .setPoolSize(10)
                .setQueueCapacity(100)
                .setQuorumName("quorum");

        Config config = new Config()
                .addExecutorConfig(expectedConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        ExecutorConfig actualConfig = xmlConfig.getExecutorConfig(expectedConfig.getName());
        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testDurableExecutor() {
        DurableExecutorConfig expectedConfig = new DurableExecutorConfig()
                .setName("testDurableExecutor")
                .setPoolSize(10)
                .setCapacity(100)
                .setDurability(2)
                .setQuorumName("quorum");

        Config config = new Config()
                .addDurableExecutorConfig(expectedConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        DurableExecutorConfig actualConfig = xmlConfig.getDurableExecutorConfig(expectedConfig.getName());
        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testPNCounter() {
        PNCounterConfig expectedConfig = new PNCounterConfig()
                .setName("testPNCounter")
                .setReplicaCount(100)
                .setQuorumName("quorum");

        Config config = new Config().addPNCounterConfig(expectedConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        PNCounterConfig actualConfig = xmlConfig.getPNCounterConfig(expectedConfig.getName());
        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testMultiMap() {
        MultiMapConfig expectedConfig = new MultiMapConfig()
                .setName("testMultiMap")
                .setBackupCount(2)
                .setAsyncBackupCount(3)
                .setValueCollectionType(MultiMapConfig.ValueCollectionType.LIST)
                .setBinary(true)
                .setStatisticsEnabled(true)
                .setQuorumName("quorum")
                .setEntryListenerConfigs(asList(new EntryListenerConfig("java.Listener", true, true)));

        Config config = new Config()
                .addMultiMapConfig(expectedConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        MultiMapConfig actualConfig = xmlConfig.getMultiMapConfig(expectedConfig.getName());
        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testAtomicLong() {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig()
                .setPolicy(DiscardMergePolicy.class.getSimpleName())
                .setBatchSize(1234);

        AtomicLongConfig expectedConfig = new AtomicLongConfig("testAtomicLongConfig")
                .setMergePolicyConfig(mergePolicyConfig)
                .setQuorumName("quorum");

        Config config = new Config()
                .addAtomicLongConfig(expectedConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        AtomicLongConfig actualConfig = xmlConfig.getAtomicLongConfig(expectedConfig.getName());
        assertEquals(expectedConfig, actualConfig);

        MergePolicyConfig xmlMergePolicyConfig = actualConfig.getMergePolicyConfig();
        assertEquals(DiscardMergePolicy.class.getSimpleName(), xmlMergePolicyConfig.getPolicy());
        assertEquals(1234, xmlMergePolicyConfig.getBatchSize());
    }

    @Test
    public void testAtomicReference() {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig()
                .setPolicy(PassThroughMergePolicy.class.getSimpleName())
                .setBatchSize(4321);

        AtomicReferenceConfig expectedConfig = new AtomicReferenceConfig("testAtomicReferenceConfig")
                .setMergePolicyConfig(mergePolicyConfig)
                .setQuorumName("quorum");

        Config config = new Config()
                .addAtomicReferenceConfig(expectedConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        AtomicReferenceConfig actualConfig = xmlConfig.getAtomicReferenceConfig(expectedConfig.getName());
        assertEquals(expectedConfig, actualConfig);

        MergePolicyConfig xmlMergePolicyConfig = actualConfig.getMergePolicyConfig();
        assertEquals(PassThroughMergePolicy.class.getSimpleName(), xmlMergePolicyConfig.getPolicy());
        assertEquals(4321, xmlMergePolicyConfig.getBatchSize());
    }

    @Test
    public void testCountDownLatch() {
        CountDownLatchConfig expectedConfig = new CountDownLatchConfig("testCountDownLatchConfig")
                .setQuorumName("quorum");


        Config config = new Config()
                .addCountDownLatchConfig(expectedConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        CountDownLatchConfig actualConfig = xmlConfig.getCountDownLatchConfig(expectedConfig.getName());
        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testList() {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig()
                .setPolicy(HigherHitsMergePolicy.class.getName())
                .setBatchSize(1234);

        ListConfig expectedConfig = new ListConfig("testList")
                .setMaxSize(10)
                .setStatisticsEnabled(true)
                .setBackupCount(2)
                .setAsyncBackupCount(3)
                .setQuorumName("quorum")
                .setMergePolicyConfig(mergePolicyConfig)
                .setItemListenerConfigs(asList(new ItemListenerConfig("java.Listener", true)));

        Config config = new Config()
                .addListConfig(expectedConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        ListConfig actualConfig = xmlConfig.getListConfig("testList");
        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testSet() {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig()
                .setPolicy(LatestUpdateMergePolicy.class.getName())
                .setBatchSize(1234);

        SetConfig expectedConfig = new SetConfig("testSet")
                .setMaxSize(10)
                .setStatisticsEnabled(true)
                .setBackupCount(2)
                .setAsyncBackupCount(3)
                .setQuorumName("quorum")
                .setMergePolicyConfig(mergePolicyConfig)
                .setItemListenerConfigs(asList(new ItemListenerConfig("java.Listener", true)));

        Config config = new Config()
                .addSetConfig(expectedConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        SetConfig actualConfig = xmlConfig.getSetConfig("testSet");
        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testQueueWithStoreClass() {
        QueueStoreConfig queueStoreConfig = new QueueStoreConfig()
                .setClassName("className")
                .setEnabled(true)
                .setProperty("key", "value");

        testQueue(queueStoreConfig);
    }

    @Test
    public void testQueueWithStoreFactory() {
        QueueStoreConfig queueStoreConfig = new QueueStoreConfig()
                .setFactoryClassName("factoryClassName")
                .setEnabled(true)
                .setProperty("key", "value");

        testQueue(queueStoreConfig);
    }

    private void testQueue(QueueStoreConfig queueStoreConfig) {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig()
                .setPolicy(DiscardMergePolicy.class.getSimpleName())
                .setBatchSize(1234);

        QueueConfig expectedConfig = new QueueConfig()
                .setName("testQueue")
                .setMaxSize(10)
                .setStatisticsEnabled(true)
                .setBackupCount(2)
                .setAsyncBackupCount(3)
                .setEmptyQueueTtl(1000)
                .setMergePolicyConfig(mergePolicyConfig)
                .setQueueStoreConfig(queueStoreConfig)
                .setItemListenerConfigs(asList(new ItemListenerConfig("java.Listener", true)));

        Config config = new Config()
                .addQueueConfig(expectedConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        QueueConfig actualConfig = xmlConfig.getQueueConfig("testQueue");
        assertEquals("testQueue", actualConfig.getName());

        MergePolicyConfig xmlMergePolicyConfig = actualConfig.getMergePolicyConfig();
        assertEquals(DiscardMergePolicy.class.getSimpleName(), xmlMergePolicyConfig.getPolicy());
        assertEquals(1234, xmlMergePolicyConfig.getBatchSize());
        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testNativeMemory() {
        NativeMemoryConfig expectedConfig = new NativeMemoryConfig();
        expectedConfig.setEnabled(true);
        expectedConfig.setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.STANDARD);
        expectedConfig.setMetadataSpacePercentage(12.5f);
        expectedConfig.setMinBlockSize(50);
        expectedConfig.setPageSize(100);
        expectedConfig.setSize(new MemorySize(20, MemoryUnit.MEGABYTES));

        Config config = new Config().setNativeMemoryConfig(expectedConfig);
        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        NativeMemoryConfig actualConfig = xmlConfig.getNativeMemoryConfig();
        assertTrue(actualConfig.isEnabled());
        assertEquals(NativeMemoryConfig.MemoryAllocatorType.STANDARD, actualConfig.getAllocatorType());
        assertEquals(12.5, actualConfig.getMetadataSpacePercentage(), 0.0001);
        assertEquals(50, actualConfig.getMinBlockSize());
        assertEquals(100, actualConfig.getPageSize());
        assertEquals(new MemorySize(20, MemoryUnit.MEGABYTES).getUnit(), actualConfig.getSize().getUnit());
        assertEquals(new MemorySize(20, MemoryUnit.MEGABYTES).getValue(), actualConfig.getSize().getValue());
        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testMapAttributesConfigWithStoreClass() {
        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setEnabled(true)
                .setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER)
                .setWriteDelaySeconds(10)
                .setClassName("className")
                .setWriteCoalescing(true)
                .setWriteBatchSize(500)
                .setProperty("key", "value");

        testMap(mapStoreConfig);
    }

    @Test
    public void testCRDTReplication() {
        final CRDTReplicationConfig replicationConfig = new CRDTReplicationConfig()
                .setMaxConcurrentReplicationTargets(10)
                .setReplicationPeriodMillis(2000);
        final Config config = new Config().setCRDTReplicationConfig(replicationConfig);
        final Config xmlConfig = getNewConfigViaXMLGenerator(config);
        final CRDTReplicationConfig xmlReplicationConfig = xmlConfig.getCRDTReplicationConfig();

        assertNotNull(xmlReplicationConfig);
        assertEquals(10, xmlReplicationConfig.getMaxConcurrentReplicationTargets());
        assertEquals(2000, xmlReplicationConfig.getReplicationPeriodMillis());
    }

    @Test
    public void testMapAttributesConfigWithStoreFactory() {
        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setEnabled(true)
                .setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER)
                .setWriteDelaySeconds(10)
                .setWriteCoalescing(true)
                .setWriteBatchSize(500)
                .setFactoryClassName("factoryClassName")
                .setProperty("key", "value");

        testMap(mapStoreConfig);
    }

    @SuppressWarnings("deprecation")
    private void testMap(MapStoreConfig mapStoreConfig) {
        MapAttributeConfig attrConfig = new MapAttributeConfig()
                .setName("power")
                .setExtractor("com.car.PowerExtractor");

        MaxSizeConfig maxSizeConfig = new MaxSizeConfig()
                .setSize(10)
                .setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE);

        MapIndexConfig mapIndexConfig = new MapIndexConfig()
                .setAttribute("attribute")
                .setOrdered(true);

        EntryListenerConfig listenerConfig = new EntryListenerConfig("com.hazelcast.entrylistener", false, false);

        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE)
                .setSize(100)
                .setComparatorClassName("comparatorClassName")
                .setEvictionPolicy(EvictionPolicy.LRU);

        PredicateConfig predicateConfig1 = new PredicateConfig();
        predicateConfig1.setClassName("className");

        PredicateConfig predicateConfig2 = new PredicateConfig();
        predicateConfig2.setSql("sqlQuery");

        QueryCacheConfig queryCacheConfig1 = new QueryCacheConfig()
                .setName("queryCache1")
                .setPredicateConfig(predicateConfig1)
                .addEntryListenerConfig(listenerConfig)
                .setBatchSize(230)
                .setDelaySeconds(20)
                .setPopulate(false)
                .setBufferSize(8)
                .setInMemoryFormat(InMemoryFormat.BINARY)
                .setEvictionConfig(evictionConfig)
                .setIncludeValue(false)
                .setCoalesce(false)
                .addIndexConfig(mapIndexConfig);

        QueryCacheConfig queryCacheConfig2 = new QueryCacheConfig()
                .setName("queryCache2")
                .setPredicateConfig(predicateConfig2)
                .addEntryListenerConfig(listenerConfig)
                .setBatchSize(500)
                .setDelaySeconds(10)
                .setPopulate(true)
                .setBufferSize(10)
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setEvictionConfig(evictionConfig)
                .setIncludeValue(true)
                .setCoalesce(true)
                .addIndexConfig(mapIndexConfig);

        MapConfig expectedConfig = new MapConfig()
                .setName("carMap")
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .setEvictionPolicy(EvictionPolicy.LRU)
                .setMaxIdleSeconds(100)
                .setTimeToLiveSeconds(1000)
                .setCacheDeserializedValues(CacheDeserializedValues.ALWAYS)
                .setStatisticsEnabled(true)
                .setReadBackupData(true)
                .setBackupCount(2)
                .setAsyncBackupCount(3)
                .setEvictionPercentage(80)
                .setMinEvictionCheckMillis(1000)
                .setOptimizeQueries(true)
                .setMapStoreConfig(mapStoreConfig)
                .setMaxSizeConfig(maxSizeConfig)
                .setWanReplicationRef(wanReplicationRef())
                .setPartitioningStrategyConfig(new PartitioningStrategyConfig("partitionStrategyClass"))
                .setHotRestartConfig(hotRestartConfig())
                .setEvictionPolicy(EvictionPolicy.LRU)
                .addEntryListenerConfig(listenerConfig)
                .setMapIndexConfigs(asList(mapIndexConfig))
                .addMapAttributeConfig(attrConfig)
                .setPartitionLostListenerConfigs(asList(new MapPartitionLostListenerConfig("partitionLostListener")));

        expectedConfig.setQueryCacheConfigs(asList(queryCacheConfig1, queryCacheConfig2));

        Config config = new Config()
                .addMapConfig(expectedConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        MapConfig actualConfig = xmlConfig.getMapConfig("carMap");
        MapAttributeConfig xmlAttrConfig = actualConfig.getMapAttributeConfigs().get(0);
        assertEquals(attrConfig.getName(), xmlAttrConfig.getName());
        assertEquals(attrConfig.getExtractor(), xmlAttrConfig.getExtractor());
        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testMapNearCacheConfig() {
        NearCacheConfig expectedConfig = new NearCacheConfig()
                .setName("nearCache")
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .setMaxIdleSeconds(42)
                .setCacheLocalEntries(true)
                .setInvalidateOnChange(true)
                .setLocalUpdatePolicy(NearCacheConfig.LocalUpdatePolicy.INVALIDATE)
                .setTimeToLiveSeconds(10)
                .setEvictionConfig(evictionConfig())
                .setSerializeKeys(true);

        MapConfig mapConfig = new MapConfig()
                .setName("nearCacheTest")
                .setNearCacheConfig(expectedConfig);

        Config config = new Config()
                .addMapConfig(mapConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        NearCacheConfig actualConfig = xmlConfig.getMapConfig("nearCacheTest").getNearCacheConfig();
        assertEquals(expectedConfig, actualConfig);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testMapNearCacheEvictionConfig() {
        NearCacheConfig expectedConfig = new NearCacheConfig()
                .setName("nearCache")
                .setMaxSize(23)
                .setEvictionPolicy("LRU");

        MapConfig mapConfig = new MapConfig()
                .setName("nearCacheTest")
                .setNearCacheConfig(expectedConfig);

        Config config = new Config()
                .addMapConfig(mapConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        NearCacheConfig actualConfig = xmlConfig.getMapConfig("nearCacheTest").getNearCacheConfig();
        assertEquals(23, actualConfig.getMaxSize());
        assertEquals("LRU", actualConfig.getEvictionPolicy());
        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testMultiMapConfig() {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig()
                .setPolicy(DiscardMergePolicy.class.getSimpleName())
                .setBatchSize(2342);

        MultiMapConfig multiMapConfig = new MultiMapConfig()
                .setName("myMultiMap")
                .setBackupCount(2)
                .setAsyncBackupCount(3)
                .setBinary(false)
                .setMergePolicyConfig(mergePolicyConfig);

        Config config = new Config().addMultiMapConfig(multiMapConfig);
        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        assertEquals(multiMapConfig, xmlConfig.getMultiMapConfig("myMultiMap"));
    }

    @Test
    public void testWanConfig() {
        HashMap<String, Comparable> props = new HashMap<String, Comparable>();
        props.put("prop1", "val1");
        props.put("prop2", "val2");
        props.put("prop3", "val3");
        WanReplicationConfig wanConfig = new WanReplicationConfig()
                .setName("testName")
                .setWanConsumerConfig(new WanConsumerConfig().setClassName("dummyClass").setProperties(props));
        WanPublisherConfig publisherConfig = new WanPublisherConfig()
                .setGroupName("dummyGroup")
                .setClassName("dummyClass")
                .setAwsConfig(getDummyAwsConfig())
                .setDiscoveryConfig(getDummyDiscoveryConfig());
        wanConfig.setWanPublisherConfigs(Collections.singletonList(publisherConfig));

        Config config = new Config().addWanReplicationConfig(wanConfig);
        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        ConfigCompatibilityChecker.checkWanConfigs(config.getWanReplicationConfigs(), xmlConfig.getWanReplicationConfigs());
    }

    @Test
    public void testMapEventJournal() {
        String mapName = "mapName";
        EventJournalConfig expectedConfig = new EventJournalConfig()
                .setMapName(mapName)
                .setEnabled(true)
                .setCapacity(123)
                .setTimeToLiveSeconds(321);
        Config config = new Config().addEventJournalConfig(expectedConfig);
        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        EventJournalConfig actualConfig = xmlConfig.getMapEventJournalConfig(mapName);
        assertTrue(new EventJournalConfigChecker().check(expectedConfig, actualConfig));
        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testCacheEventJournal() {
        String cacheName = "cacheName";
        EventJournalConfig expectedConfig = new EventJournalConfig()
                .setCacheName(cacheName)
                .setEnabled(true)
                .setCapacity(123)
                .setTimeToLiveSeconds(321);
        Config config = new Config().addEventJournalConfig(expectedConfig);
        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        EventJournalConfig actualConfig = xmlConfig.getCacheEventJournalConfig(cacheName);
        assertTrue(new EventJournalConfigChecker().check(expectedConfig, actualConfig));
        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testCardinalityEstimator() {
        Config cfg = new Config();
        CardinalityEstimatorConfig estimatorConfig = new CardinalityEstimatorConfig()
                .setBackupCount(2)
                .setAsyncBackupCount(3)
                .setName("Existing")
                .setQuorumName("quorum")
                .setMergePolicyConfig(new MergePolicyConfig("DiscardMergePolicy", 14));
        cfg.addCardinalityEstimatorConfig(estimatorConfig);

        CardinalityEstimatorConfig defaultCardinalityEstConfig = new CardinalityEstimatorConfig();
        cfg.addCardinalityEstimatorConfig(defaultCardinalityEstConfig);

        CardinalityEstimatorConfig existing = getNewConfigViaXMLGenerator(cfg).getCardinalityEstimatorConfig("Existing");
        assertEquals(estimatorConfig, existing);

        CardinalityEstimatorConfig fallsbackToDefault = getNewConfigViaXMLGenerator(cfg)
                .getCardinalityEstimatorConfig("NotExisting/Default");
        assertEquals(defaultCardinalityEstConfig.getMergePolicyConfig(), fallsbackToDefault.getMergePolicyConfig());
        assertEquals(defaultCardinalityEstConfig.getBackupCount(), fallsbackToDefault.getBackupCount());
        assertEquals(defaultCardinalityEstConfig.getAsyncBackupCount(), fallsbackToDefault.getAsyncBackupCount());
        assertEquals(defaultCardinalityEstConfig.getQuorumName(), fallsbackToDefault.getQuorumName());
    }

    @Test
    public void testTopicGlobalOrdered() {
        Config cfg = new Config();

        TopicConfig expectedConfig = new TopicConfig()
                .setName("TestTopic")
                .setGlobalOrderingEnabled(true)
                .setStatisticsEnabled(true)
                .setMessageListenerConfigs(asList(new ListenerConfig("foo.bar.Listener")));
        cfg.addTopicConfig(expectedConfig);

        TopicConfig actualConfig = getNewConfigViaXMLGenerator(cfg).getTopicConfig("TestTopic");

        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testTopicMultiThreaded() {
        String testTopic = "TestTopic";
        Config cfg = new Config();

        TopicConfig expectedConfig = new TopicConfig()
                .setName(testTopic)
                .setMultiThreadingEnabled(true)
                .setStatisticsEnabled(true)
                .setMessageListenerConfigs(asList(new ListenerConfig("foo.bar.Listener")));
        cfg.addTopicConfig(expectedConfig);

        TopicConfig actualConfig = getNewConfigViaXMLGenerator(cfg).getTopicConfig(testTopic);

        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testReliableTopic() {
        String testTopic = "TestTopic";
        Config cfg = new Config();

        ReliableTopicConfig expectedConfig = new ReliableTopicConfig()
                .setName(testTopic)
                .setReadBatchSize(10)
                .setTopicOverloadPolicy(TopicOverloadPolicy.BLOCK)
                .setStatisticsEnabled(true)
                .setMessageListenerConfigs(asList(new ListenerConfig("foo.bar.Listener")));

        cfg.addReliableTopicConfig(expectedConfig);

        ReliableTopicConfig actualConfig = getNewConfigViaXMLGenerator(cfg).getReliableTopicConfig(testTopic);

        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testLock() {
        String testLock = "TestLock";
        Config cfg = new Config();

        LockConfig expectedConfig = new LockConfig().setName(testLock).setQuorumName("quorum");

        cfg.addLockConfig(expectedConfig);

        LockConfig actualConfig = getNewConfigViaXMLGenerator(cfg).getLockConfig(testLock);

        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testScheduledExecutor() {
        Config cfg = new Config();
        ScheduledExecutorConfig scheduledExecutorConfig =
                new ScheduledExecutorConfig()
                        .setCapacity(1)
                        .setDurability(2)
                        .setName("Existing")
                        .setPoolSize(3)
                        .setQuorumName("quorum")
                        .setMergePolicyConfig(new MergePolicyConfig("JediPolicy", 23));
        cfg.addScheduledExecutorConfig(scheduledExecutorConfig);

        ScheduledExecutorConfig defaultSchedExecConfig = new ScheduledExecutorConfig();
        cfg.addScheduledExecutorConfig(defaultSchedExecConfig);

        ScheduledExecutorConfig existing = getNewConfigViaXMLGenerator(cfg).getScheduledExecutorConfig("Existing");
        assertEquals(scheduledExecutorConfig, existing);

        ScheduledExecutorConfig fallsbackToDefault = getNewConfigViaXMLGenerator(cfg)
                .getScheduledExecutorConfig("NotExisting/Default");
        assertEquals(defaultSchedExecConfig.getMergePolicyConfig(), fallsbackToDefault.getMergePolicyConfig());
        assertEquals(defaultSchedExecConfig.getCapacity(), fallsbackToDefault.getCapacity());
        assertEquals(defaultSchedExecConfig.getPoolSize(), fallsbackToDefault.getPoolSize());
        assertEquals(defaultSchedExecConfig.getDurability(), fallsbackToDefault.getDurability());
    }

    @Test
    public void testQuorumConfig_configByClassName() {
        Config config = new Config();
        QuorumConfig quorumConfig = new QuorumConfig("test-quorum", true, 3);
        quorumConfig.setType(QuorumType.READ_WRITE)
                .setQuorumFunctionClassName("com.hazelcast.QuorumFunction");
        config.addQuorumConfig(quorumConfig);

        QuorumConfig generatedConfig = getNewConfigViaXMLGenerator(config).getQuorumConfig("test-quorum");
        assertTrue(generatedConfig.toString() + " should be compatible with " + quorumConfig.toString(),
                new ConfigCompatibilityChecker.QuorumConfigChecker().check(quorumConfig, generatedConfig));
    }

    @Test
    public void testQuorumConfig_configuredByRecentlyActiveQuorumConfigBuilder() {
        Config config = new Config();
        QuorumConfig quorumConfig = QuorumConfig.newRecentlyActiveQuorumConfigBuilder("recently-active", 3, 3141592)
                .build();
        quorumConfig.setType(QuorumType.READ_WRITE)
                .addListenerConfig(new QuorumListenerConfig("com.hazelcast.QuorumListener"));
        config.addQuorumConfig(quorumConfig);

        QuorumConfig generatedConfig = getNewConfigViaXMLGenerator(config).getQuorumConfig("recently-active");
        assertTrue(generatedConfig.toString() + " should be compatible with " + quorumConfig.toString(),
                new ConfigCompatibilityChecker.QuorumConfigChecker().check(quorumConfig, generatedConfig));
    }

    @Test
    public void testQuorumConfig_configuredByProbabilisticQuorumConfigBuilder() {
        Config config = new Config();
        QuorumConfig quorumConfig = QuorumConfig.newProbabilisticQuorumConfigBuilder("probabilistic-quorum", 3)
                .withHeartbeatIntervalMillis(1)
                .withAcceptableHeartbeatPauseMillis(2)
                .withMaxSampleSize(3)
                .withMinStdDeviationMillis(4)
                .withSuspicionThreshold(5)
                .build();
        quorumConfig.setType(QuorumType.READ_WRITE)
                .addListenerConfig(new QuorumListenerConfig("com.hazelcast.QuorumListener"));
        config.addQuorumConfig(quorumConfig);

        QuorumConfig generatedConfig = getNewConfigViaXMLGenerator(config).getQuorumConfig("probabilistic-quorum");
        assertTrue(generatedConfig.toString() + " should be compatible with " + quorumConfig.toString(),
                new ConfigCompatibilityChecker.QuorumConfigChecker().check(quorumConfig, generatedConfig));
    }

    private DiscoveryConfig getDummyDiscoveryConfig() {
        DiscoveryStrategyConfig strategyConfig = new DiscoveryStrategyConfig("dummyClass");
        strategyConfig.addProperty("prop1", "val1");
        strategyConfig.addProperty("prop2", "val2");

        DiscoveryConfig discoveryConfig = new DiscoveryConfig();
        discoveryConfig.setNodeFilterClass("dummyNodeFilter");
        discoveryConfig.addDiscoveryStrategyConfig(strategyConfig);
        discoveryConfig.addDiscoveryStrategyConfig(new DiscoveryStrategyConfig("dummyClass2"));

        return discoveryConfig;
    }

    private AwsConfig getDummyAwsConfig() {
        return new AwsConfig().setHostHeader("dummyHost")
                .setRegion("dummyRegion")
                .setEnabled(false)
                .setConnectionTimeoutSeconds(1)
                .setAccessKey("dummyKey")
                .setIamRole("dummyIam")
                .setSecretKey("dummySecretKey")
                .setSecurityGroupName("dummyGroupName")
                .setTagKey("dummyTagKey")
                .setTagValue("dummyTagValue");
    }

    private static Config getNewConfigViaXMLGenerator(Config config) {
        return getNewConfigViaXMLGenerator(config, true);
    }

    private static Config getNewConfigViaXMLGenerator(Config config, boolean maskSensitiveFields) {
        ConfigXmlGenerator configXmlGenerator = new ConfigXmlGenerator(true, maskSensitiveFields);
        String xml = configXmlGenerator.generate(config);

        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        return configBuilder.build();
    }

    private static WanReplicationRef wanReplicationRef() {
        return new WanReplicationRef()
                .setName("wanReplication")
                .setMergePolicy("mergePolicy")
                .setRepublishingEnabled(true)
                .setFilters(Arrays.asList("filter1", "filter2"));
    }

    private static HotRestartConfig hotRestartConfig() {
        return new HotRestartConfig()
                .setEnabled(true)
                .setFsync(true);
    }

    private static EvictionConfig evictionConfig() {
        return new EvictionConfig()
                .setEvictionPolicy(EvictionPolicy.LRU)
                .setComparatorClassName("comparatorClassName")
                .setSize(10)
                .setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE);
    }
}
