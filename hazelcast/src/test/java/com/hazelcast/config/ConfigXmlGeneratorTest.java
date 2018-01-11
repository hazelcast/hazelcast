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

import com.hazelcast.config.ConfigCompatibilityChecker.EventJournalConfigChecker;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;

import static com.hazelcast.config.ConfigXmlGenerator.MASK_FOR_SENSITIVE_DATA;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ConfigXmlGeneratorTest {

    @Test
    public void testIfSensitiveDataIsMasked() {
        Config cfg = new Config();
        SSLConfig sslConfig = new SSLConfig();
        sslConfig.setProperty("keyStorePassword", "Hazelcast");
        cfg.getNetworkConfig().setSSLConfig(sslConfig);

        SymmetricEncryptionConfig symmetricEncryptionConfig = new SymmetricEncryptionConfig();
        symmetricEncryptionConfig.setPassword("Hazelcast");
        symmetricEncryptionConfig.setSalt("theSalt");

        cfg.getNetworkConfig().setSymmetricEncryptionConfig(symmetricEncryptionConfig);
        cfg.setLicenseKey("HazelcastLicenseKey");

        Config newConfigViaXMLGenerator = getNewConfigViaXMLGenerator(cfg);
        SSLConfig generatedSSLConfig = newConfigViaXMLGenerator.getNetworkConfig().getSSLConfig();

        assertEquals(generatedSSLConfig.getProperty("keyStorePassword"), MASK_FOR_SENSITIVE_DATA);

        String secPassword = newConfigViaXMLGenerator.getNetworkConfig().getSymmetricEncryptionConfig().getPassword();
        String theSalt = newConfigViaXMLGenerator.getNetworkConfig().getSymmetricEncryptionConfig().getSalt();
        assertEquals(secPassword, MASK_FOR_SENSITIVE_DATA);
        assertEquals(theSalt, MASK_FOR_SENSITIVE_DATA);
        assertEquals(newConfigViaXMLGenerator.getLicenseKey(), MASK_FOR_SENSITIVE_DATA);
        assertEquals(newConfigViaXMLGenerator.getGroupConfig().getPassword(), MASK_FOR_SENSITIVE_DATA);
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
        ReplicatedMapConfig replicatedMapConfig = new ReplicatedMapConfig()
                .setName("replicated-map-name")
                .setStatisticsEnabled(false)
                .setConcurrencyLevel(128)
                .setQuorumName("quorum")
                .addEntryListenerConfig(new EntryListenerConfig("com.hazelcast.entrylistener", false, false));

        Config config = new Config()
                .addReplicatedMapConfig(replicatedMapConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        ReplicatedMapConfig xmlReplicatedMapConfig = xmlConfig.getReplicatedMapConfig("replicated-map-name");
        assertEquals("replicated-map-name", xmlReplicatedMapConfig.getName());
        assertFalse(xmlReplicatedMapConfig.isStatisticsEnabled());
        assertEquals(128, xmlReplicatedMapConfig.getConcurrencyLevel());
        assertEquals("com.hazelcast.entrylistener", xmlReplicatedMapConfig.getListenerConfigs().get(0).getClassName());
        assertEquals("quorum", xmlReplicatedMapConfig.getQuorumName());
    }

    @Test
    public void testFlakeIdGeneratorConfigGenerator() {
        FlakeIdGeneratorConfig figConfig = new FlakeIdGeneratorConfig("flake-id-gen1")
                .setPrefetchCount(3)
                .setPrefetchValidityMillis(10L);

        Config config = new Config()
                .addFlakeIdGeneratorConfig(figConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        FlakeIdGeneratorConfig xmlReplicatedConfig = xmlConfig.getFlakeIdGeneratorConfig("flake-id-gen1");
        assertEquals(figConfig, xmlReplicatedConfig);
    }

    @Test
    public void testCacheQuorumRef() {
        CacheSimpleConfig cacheConfig = new CacheSimpleConfig()
                .setName("testCache")
                .setQuorumName("testQuorum");

        Config config = new Config()
                .addCacheConfig(cacheConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        CacheSimpleConfig xmlCacheConfig = xmlConfig.getCacheConfig("testCache");
        assertEquals("testQuorum", xmlCacheConfig.getQuorumName());
    }

    @Test
    public void testRingbuffer() {
        RingbufferStoreConfig ringbufferStoreConfig = new RingbufferStoreConfig()
                .setEnabled(true)
                .setClassName("ClassName")
                .setProperty("p1", "v1")
                .setProperty("p2", "v2")
                .setProperty("p3", "v3");
        RingbufferConfig rbConfig = new RingbufferConfig("testRbConfig")
                .setBackupCount(1)
                .setAsyncBackupCount(2)
                .setCapacity(3)
                .setTimeToLiveSeconds(4)
                .setInMemoryFormat(InMemoryFormat.BINARY)
                .setRingbufferStoreConfig(ringbufferStoreConfig)
                .setQuorumName("quorum");

        Config config = new Config().addRingBufferConfig(rbConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        RingbufferConfig xmlRbConfig = xmlConfig.getRingbufferConfig(rbConfig.getName());
        assertEquals(rbConfig, xmlRbConfig);
    }

    @Test
    public void testSemaphore() {
        // TODO -> not full config checked
        SemaphoreConfig expectedConfig = new SemaphoreConfig()
                .setName("testSemaphore")
                .setQuorumName("quorum");

        Config config = new Config()
                .addSemaphoreConfig(expectedConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        SemaphoreConfig actualConfig = xmlConfig.getSemaphoreConfig(expectedConfig.getName());
        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testSet() {
        // TODO -> not full config checked
        SetConfig expectedConfig = new SetConfig()
                .setName("testSemaphore")
                .setQuorumName("quorum");

        Config config = new Config()
                .addSetConfig(expectedConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        SetConfig actualConfig = xmlConfig.getSetConfig(expectedConfig.getName());
        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testList() {
        // TODO -> not full config checked
        ListConfig expectedConfig = new ListConfig()
                .setName("testSemaphore")
                .setQuorumName("quorum");

        Config config = new Config()
                .addListConfig(expectedConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        ListConfig actualConfig = xmlConfig.getListConfig(expectedConfig.getName());
        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testExecutor() {
        // TODO -> not full config checked
        ExecutorConfig expectedConfig = new ExecutorConfig()
                .setName("testSemaphore")
                .setQuorumName("quorum");

        Config config = new Config()
                .addExecutorConfig(expectedConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        ExecutorConfig actualConfig = xmlConfig.getExecutorConfig(expectedConfig.getName());
        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testDurableExecutor() {
        // TODO -> not full config checked
        DurableExecutorConfig expectedConfig = new DurableExecutorConfig()
                .setName("testSemaphore")
                .setQuorumName("quorum");

        Config config = new Config()
                .addDurableExecutorConfig(expectedConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        DurableExecutorConfig actualConfig = xmlConfig.getDurableExecutorConfig(expectedConfig.getName());
        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testScheduledExecutor() {
        // TODO -> not full config checked
        ScheduledExecutorConfig expectedConfig = new ScheduledExecutorConfig()
                .setName("testSemaphore")
                .setQuorumName("quorum");

        Config config = new Config()
                .addScheduledExecutorConfig(expectedConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        ScheduledExecutorConfig actualConfig = xmlConfig.getScheduledExecutorConfig(expectedConfig.getName());
        assertEquals(expectedConfig, actualConfig);
    }

    @Test
    public void testMultiMap() {
        // TODO -> not full config checked
        MultiMapConfig expectedConfig = new MultiMapConfig()
                .setName("testSemaphore")
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
    public void testCacheMergePolicy() {
        CacheSimpleConfig cacheConfig = new CacheSimpleConfig();
        cacheConfig.setName("testCache");
        cacheConfig.setMergePolicy("testMergePolicy");

        Config config = new Config()
                .addCacheConfig(cacheConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        CacheSimpleConfig xmlCacheConfig = xmlConfig.getCacheConfig("testCache");
        assertEquals("testMergePolicy", xmlCacheConfig.getMergePolicy());
    }

    @Test
    public void testNativeMemory() {
        NativeMemoryConfig nativeMemoryConfig = new NativeMemoryConfig();
        nativeMemoryConfig.setEnabled(true);
        nativeMemoryConfig.setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.STANDARD);
        nativeMemoryConfig.setMetadataSpacePercentage((float) 12.5);
        nativeMemoryConfig.setMinBlockSize(50);
        nativeMemoryConfig.setPageSize(100);
        nativeMemoryConfig.setSize(new MemorySize(20, MemoryUnit.MEGABYTES));

        Config config = new Config()
                .setNativeMemoryConfig(nativeMemoryConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        NativeMemoryConfig xmlNativeMemoryConfig = xmlConfig.getNativeMemoryConfig();
        assertTrue(xmlNativeMemoryConfig.isEnabled());
        assertEquals(NativeMemoryConfig.MemoryAllocatorType.STANDARD, nativeMemoryConfig.getAllocatorType());
        assertEquals(12.5, nativeMemoryConfig.getMetadataSpacePercentage(), 0.0001);
        assertEquals(50, nativeMemoryConfig.getMinBlockSize());
        assertEquals(100, nativeMemoryConfig.getPageSize());
        assertEquals(new MemorySize(20, MemoryUnit.MEGABYTES).getUnit(), nativeMemoryConfig.getSize().getUnit());
        assertEquals(new MemorySize(20, MemoryUnit.MEGABYTES).getValue(), nativeMemoryConfig.getSize().getValue());
    }

    @Test
    public void testMapAttributesConfig() {
        MapAttributeConfig attrConfig = new MapAttributeConfig()
                .setName("power")
                .setExtractor("com.car.PowerExtractor");

        MapConfig mapConfig = new MapConfig()
                .setName("carMap")
                .addMapAttributeConfig(attrConfig);

        Config config = new Config()
                .addMapConfig(mapConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        MapAttributeConfig xmlAttrConfig = xmlConfig.getMapConfig("carMap").getMapAttributeConfigs().get(0);
        assertEquals(attrConfig.getName(), xmlAttrConfig.getName());
        assertEquals(attrConfig.getExtractor(), xmlAttrConfig.getExtractor());
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testMapNearCacheConfig() {
        NearCacheConfig nearCacheConfig = new NearCacheConfig()
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .setMaxSize(23)
                .setEvictionPolicy("LRU")
                .setMaxIdleSeconds(42)
                .setCacheLocalEntries(true);

        MapConfig mapConfig = new MapConfig()
                .setName("nearCacheTest")
                .setNearCacheConfig(nearCacheConfig);

        Config config = new Config()
                .addMapConfig(mapConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        NearCacheConfig xmlNearCacheConfig = xmlConfig.getMapConfig("nearCacheTest").getNearCacheConfig();
        assertEquals(InMemoryFormat.NATIVE, xmlNearCacheConfig.getInMemoryFormat());
        assertEquals(23, xmlNearCacheConfig.getMaxSize());
        assertEquals(23, xmlNearCacheConfig.getEvictionConfig().getSize());
        assertEquals("LRU", xmlNearCacheConfig.getEvictionPolicy());
        assertEquals(EvictionPolicy.LRU, xmlNearCacheConfig.getEvictionConfig().getEvictionPolicy());
        assertEquals(42, xmlNearCacheConfig.getMaxIdleSeconds());
        assertTrue(xmlNearCacheConfig.isCacheLocalEntries());
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
        EventJournalConfig journalConfig = new EventJournalConfig()
                .setMapName(mapName)
                .setEnabled(true)
                .setCapacity(123)
                .setTimeToLiveSeconds(321);
        Config config = new Config().addEventJournalConfig(journalConfig);
        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        assertTrue(new EventJournalConfigChecker().check(
                journalConfig,
                xmlConfig.getMapEventJournalConfig(mapName)));
    }

    @Test
    public void testCacheEventJournal() {
        String cacheName = "cacheName";
        EventJournalConfig journalConfig = new EventJournalConfig()
                .setCacheName(cacheName)
                .setEnabled(true)
                .setCapacity(123)
                .setTimeToLiveSeconds(321);
        Config config = new Config().addEventJournalConfig(journalConfig);
        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        assertTrue(new EventJournalConfigChecker().check(
                journalConfig,
                xmlConfig.getCacheEventJournalConfig(cacheName)));
    }

    @Test
    public void testTlsHostVerification() {
        Config cfg = new Config();
        SSLConfig sslConfig = new SSLConfig();
        HostVerificationConfig hostVerification = new HostVerificationConfig();
        hostVerification.setEnabledOnServer(true);
        hostVerification.setPolicyClassName("example.mycompany.FooVerifier");
        hostVerification.setProperty("test", "value");
        sslConfig.setHostVerificationConfig(hostVerification);
        cfg.getNetworkConfig().setSSLConfig(sslConfig);

        HostVerificationConfig generatedConfig = getNewConfigViaXMLGenerator(cfg).getNetworkConfig().getSSLConfig()
                .getHostVerificationConfig();
        assertNotNull(generatedConfig);
        assertEquals("example.mycompany.FooVerifier", generatedConfig.getPolicyClassName());
        assertTrue(generatedConfig.isEnabledOnServer());
        assertEquals("value", generatedConfig.getProperties().get("test"));

    }

    @Test
    public void testCardinalityEstimator() {
        Config cfg = new Config();
        CardinalityEstimatorConfig estimatorConfig =
                new CardinalityEstimatorConfig()
                        .setBackupCount(2)
                        .setAsyncBackupCount(3)
                        .setName("Existing")
                        .setQuorumName("quorum")
                        .setMergePolicyConfig(new MergePolicyConfig("Policy has no name", 14));
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
        ConfigXmlGenerator configXmlGenerator = new ConfigXmlGenerator();
        String xml = configXmlGenerator.generate(config);
        
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        return configBuilder.build();
    }
}
