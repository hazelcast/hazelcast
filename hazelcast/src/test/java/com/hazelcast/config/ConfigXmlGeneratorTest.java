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

package com.hazelcast.config;

import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.util.Collections;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
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

        assertEquals(generatedSSLConfig.getProperty("keyStorePassword"), ConfigXmlGenerator.MASK_FOR_SESITIVE_DATA);

        String secPassword = newConfigViaXMLGenerator.getNetworkConfig().getSymmetricEncryptionConfig().getPassword();
        String theSalt = newConfigViaXMLGenerator.getNetworkConfig().getSymmetricEncryptionConfig().getSalt();
        assertEquals(secPassword, ConfigXmlGenerator.MASK_FOR_SESITIVE_DATA);
        assertEquals(theSalt, ConfigXmlGenerator.MASK_FOR_SESITIVE_DATA);
        assertEquals(newConfigViaXMLGenerator.getLicenseKey(), ConfigXmlGenerator.MASK_FOR_SESITIVE_DATA);
        assertEquals(newConfigViaXMLGenerator.getGroupConfig().getPassword(), ConfigXmlGenerator.MASK_FOR_SESITIVE_DATA);
    }

    @Test
    public void testReplicatedMapConfigGenerator() {
        ReplicatedMapConfig replicatedMapConfig = new ReplicatedMapConfig()
                .setName("replicated-map-name")
                .setStatisticsEnabled(false)
                .setConcurrencyLevel(128)
                .addEntryListenerConfig(new EntryListenerConfig("com.hazelcast.entrylistener", false, false));

        Config config = new Config()
                .addReplicatedMapConfig(replicatedMapConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        ReplicatedMapConfig xmlReplicatedMapConfig = xmlConfig.getReplicatedMapConfig("replicated-map-name");
        assertEquals("replicated-map-name", xmlReplicatedMapConfig.getName());
        assertEquals(false, xmlReplicatedMapConfig.isStatisticsEnabled());
        assertEquals(128, xmlReplicatedMapConfig.getConcurrencyLevel());
        assertEquals("com.hazelcast.entrylistener", xmlReplicatedMapConfig.getListenerConfigs().get(0).getClassName());
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
        assertEquals(true, xmlNativeMemoryConfig.isEnabled());
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
        final HashMap<String, Comparable> props = new HashMap<String, Comparable>();
        props.put("prop1", "val1");
        props.put("prop2", "val2");
        props.put("prop3", "val3");
        final WanReplicationConfig wanConfig = new WanReplicationConfig()
                .setName("testName")
                .setWanConsumerConfig(new WanConsumerConfig().setClassName("dummyClass").setProperties(props));
        final WanPublisherConfig publisherConfig = new WanPublisherConfig()
                .setGroupName("dummyGroup")
                .setClassName("dummyClass")
                .setAwsConfig(getDummyAwsConfig())
                .setDiscoveryConfig(getDummyDiscoveryConfig());
        wanConfig.setWanPublisherConfigs(Collections.singletonList(publisherConfig));

        final Config config = new Config().addWanReplicationConfig(wanConfig);
        final Config xmlConfig = getNewConfigViaXMLGenerator(config);

        ConfigCompatibilityChecker.checkWanConfigs(config.getWanReplicationConfigs(), xmlConfig.getWanReplicationConfigs());
    }

    private DiscoveryConfig getDummyDiscoveryConfig() {
        final DiscoveryStrategyConfig strategyConfig = new DiscoveryStrategyConfig("dummyClass");
        strategyConfig.addProperty("prop1", "val1");
        strategyConfig.addProperty("prop2", "val2");
        final DiscoveryConfig c = new DiscoveryConfig();
        c.setNodeFilterClass("dummyNodeFilter");
        c.addDiscoveryStrategyConfig(strategyConfig);
        c.addDiscoveryStrategyConfig(new DiscoveryStrategyConfig("dummyClass2"));
        return c;
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
