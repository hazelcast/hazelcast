/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.longrunning;

import com.hazelcast.client.ClientConfig;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import org.junit.After;
import org.junit.Test;

import java.util.Map;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotSame;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public class ClientMapGuavaCacheTest {

    @After
    public void shutdown(){
        Hazelcast.shutdownAll();
    }

    @Test
    public void testReadFromCache() throws InterruptedException {
        System.setProperty("hazelcast.client.near.cache.enabled", "true");
        Config config = new Config();
        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setMaxSize(1000);
        nearCacheConfig.setMaxIdleSeconds(10);
        nearCacheConfig.setTimeToLiveSeconds(20);
        config.getMapConfig("default").setNearCacheConfig(nearCacheConfig);
        HazelcastInstance h = Hazelcast.newHazelcastInstance(config);
        String mapName = "testReadFromCache";
        IMap<String, String> mapH = h.getMap(mapName);
        mapH.put("a", "a");
        HazelcastClient client = HazelcastClient.newHazelcastClient(new ClientConfig());
        Map<Object, Object> mapC = client.getMap(mapName);

        assertEquals("a", mapC.get("a"));
        int hit = mapH.getMapEntry("a").getHits();
        mapC.get("a");
        assertEquals(mapH.getMapEntry("a").getHits(), hit);
        for (int i = 0; i < 100; i++) {
            mapC.get("a");
        }
        assertEquals(mapH.getMapEntry("a").getHits(), hit);
        
        mapH.remove("a");
        Thread.sleep(100);
        assertNull(mapC.get("a"));
        mapC.put("a", "a");
        Thread.sleep(100);
        assertEquals("a", mapC.get("a"));
    }

    @Test
    public void testNoNearCacheConfig() {
        System.setProperty("hazelcast.client.near.cache.enabled", "false");
        Config config = new Config();
        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setMaxSize(1000);
        nearCacheConfig.setMaxIdleSeconds(10);
        nearCacheConfig.setTimeToLiveSeconds(20);
        config.getMapConfig("default").setNearCacheConfig(nearCacheConfig);
        HazelcastInstance h = Hazelcast.newHazelcastInstance(config);
        String mapName = "testReadFromCache";
        IMap<String, String> mapH = h.getMap(mapName);
        mapH.put("a", "a");
        HazelcastClient client = HazelcastClient.newHazelcastClient(new ClientConfig());
        Map<Object, Object> mapC = client.getMap(mapName);

        assertEquals("a", mapC.get("a"));
        int hit = mapH.getMapEntry("a").getHits();
        mapC.get("a");
        assertEquals(mapH.getMapEntry("a").getHits(), ++hit);
        for (int i = 0; i < 100; i++) {
            mapC.get("a");
        }
        assertEquals(mapH.getMapEntry("a").getHits(), hit + 100);
    }

    @Test
    public void testInvalidateOnChange() throws InterruptedException {
        System.setProperty("hazelcast.client.near.cache.enabled", "true");
        Config config = new Config();
        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setMaxSize(1000);
        nearCacheConfig.setInvalidateOnChange(true);
        config.getMapConfig("default").setNearCacheConfig(nearCacheConfig);
        HazelcastInstance h = Hazelcast.newHazelcastInstance(config);
        String mapName = "testInvalidateOnChange";
        IMap<String, String> mapH = h.getMap(mapName);
        mapH.put("a", "a");
        HazelcastClient client = HazelcastClient.newHazelcastClient(new ClientConfig());
        Map<Object, Object> mapC = client.getMap(mapName);
        int hit = mapH.getMapEntry("a").getHits();
        assertEquals("a", mapC.get("a"));
        assertEquals(mapH.getMapEntry("a").getHits(), ++hit);
        mapC.get("a");
        assertEquals(mapH.getMapEntry("a").getHits(), hit);
        mapH.put("a", "b");
        Thread.sleep(100);
        assertEquals("b", mapC.get("a"));
        assertEquals(mapH.getMapEntry("a").getHits(), ++hit);
        
        mapH.remove("a");
        Thread.sleep(100);
        assertNull(mapC.get("a"));
        mapH.put("a", "a");
        Thread.sleep(100);
        assertEquals("a", mapC.get("a"));
    }
}
