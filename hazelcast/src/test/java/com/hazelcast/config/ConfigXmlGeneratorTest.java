/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ConfigXmlGeneratorTest {

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
                .setMaxIdleSeconds(42);

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
    }

    private static Config getNewConfigViaXMLGenerator(Config config) {
        ConfigXmlGenerator configXmlGenerator = new ConfigXmlGenerator();
        String xml = configXmlGenerator.generate(config);

        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        return configBuilder.build();
    }
}
