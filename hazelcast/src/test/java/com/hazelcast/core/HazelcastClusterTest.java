/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.core;

import com.hazelcast.config.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;

import static junit.framework.Assert.*;
import static org.junit.Assert.assertEquals;

/**
 * HazelcastTest tests some specific cluster behavior.
 * Node is created for each test method.
 */
@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class HazelcastClusterTest {

    @Before
    @After
    public void init() throws Exception {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testUseBackupDataGet() throws Exception {
        final Config config = new Config();
        final MapConfig mapConfig = new MapConfig();
        mapConfig.setName("q");
        mapConfig.setReadBackupData(true);
        config.setMapConfigs(Collections.singletonMap(mapConfig.getName(), mapConfig));
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        h1.getMap("q").put("q", "Q");
        Thread.sleep(50L);
        final IMap<Object, Object> map2 = h2.getMap("q");
        assertEquals("Q", map2.get("q"));
    }

    @Test
    public void testJoinWithCompatibleConfigs() throws Exception {
        Config config = new XmlConfigBuilder().build();
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        Thread.sleep(1000);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        final int s1 = h1.getCluster().getMembers().size();
        final int s2 = h2.getCluster().getMembers().size();
        assertEquals(s1, s2);
        assertEquals(2, s2);
    }

    @Test
    public void testJoinWithIncompatibleConfigs() throws Exception {
        Config config1 = new XmlConfigBuilder().build();
        Config config2 = new XmlConfigBuilder().build();
        config2.getMapConfig("default").setTimeToLiveSeconds(1);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config1);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config2);
        final int s1 = h1.getCluster().getMembers().size();
        final int s2 = h2.getCluster().getMembers().size();
        assertEquals(1, s1);
        assertEquals(1, s2);
    }
    
    @Test
    public void testJoinWithIncompatibleConfigsWithDisabledCheck() throws Exception {
        Config config1 = new XmlConfigBuilder().build();
        Config config2 = new XmlConfigBuilder().build();
        config1.setCheckCompatibility(false);
        config2.setCheckCompatibility(false).getMapConfig("default").setTimeToLiveSeconds(1);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config1);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config2);
        final int s1 = h1.getCluster().getMembers().size();
        final int s2 = h2.getCluster().getMembers().size();
        assertEquals(2, s1);
        assertEquals(2, s2);
    }
    
    @Test
    public void testJoinWithPostConfiguration() throws Exception {
        // issue 473
        Config hzConfig = new Config().
            setGroupConfig(new GroupConfig("foo-group")).
            setPort(5701).setPortAutoIncrement(false);
        hzConfig.getNetworkConfig().setJoin(
            new Join().
                setMulticastConfig(new MulticastConfig().setEnabled(false)).
                setTcpIpConfig(new TcpIpConfig().setMembers(Arrays.asList("127.0.0.1:5702"))));
    
        Config hzConfig2 = new Config().
            setGroupConfig(new GroupConfig("foo-group")).
            setPort(5702).setPortAutoIncrement(false);
    
        hzConfig2.getNetworkConfig().setJoin(
                new Join().
                    setMulticastConfig(new MulticastConfig().setEnabled(false)).
                    setTcpIpConfig(new TcpIpConfig().setMembers(Arrays.asList("127.0.0.1:5701"))));
        
        final HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(hzConfig);
     // Create the configuration for a dynamic map.
        instance1.getConfig().addMapConfig(new MapConfig("foo").setTimeToLiveSeconds(10));
        final IMap<Object, Object> map1 = instance1.getMap("foo");
        map1.put("issue373", "ok");
        
        final HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(hzConfig2);
        assertEquals(2, instance2.getCluster().getMembers().size());
        assertEquals("ok", instance2.getMap("foo").get("issue373"));
    }

    @Test
    public void testMapPutAndGetUseBackupData() throws Exception {
        Config config = new XmlConfigBuilder().build();
        String mapName1 = "testMapPutAndGetUseBackupData";
        String mapName2 = "testMapPutAndGetUseBackupData2";
        MapConfig mapConfig1 = new MapConfig();
        mapConfig1.setName(mapName1);
        mapConfig1.setReadBackupData(true);
        MapConfig mapConfig2 = new MapConfig();
        mapConfig2.setName(mapName2);
        mapConfig2.setReadBackupData(false);
        config.addMapConfig(mapConfig1);
        config.addMapConfig(mapConfig2);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        IMap<Object, Object> m1 = h1.getMap(mapName1);
        IMap<Object, Object> m2 = h1.getMap(mapName2);
        m1.put(1, 1);
        m2.put(1, 1);
        assertEquals(1, m1.get(1));
        assertEquals(1, m1.get(1));
        assertEquals(1, m1.get(1));
        assertEquals(1, m2.get(1));
        assertEquals(1, m2.get(1));
        assertEquals(1, m2.get(1));
        assertEquals(3, m1.getLocalMapStats().getHits());
        assertEquals(3, m2.getLocalMapStats().getHits());
    }

    @Test
    public void testLockKeyWithUseBackupData() {
        Config config = new XmlConfigBuilder().build();
        String mapName1 = "testLockKeyWithUseBackupData";
        MapConfig mapConfig1 = new MapConfig();
        mapConfig1.setName(mapName1);
        mapConfig1.setReadBackupData(true);
        config.addMapConfig(mapConfig1);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        IMap<String, String> map = h1.getMap(mapName1);
        map.lock("Hello");
        try {
            assertFalse(map.containsKey("Hello"));
        } finally {
            map.unlock("Hello");
        }
        map.put("Hello", "World");
        map.lock("Hello");
        try {
            assertTrue(map.containsKey("Hello"));
        } finally {
            map.unlock("Hello");
        }
        map.remove("Hello");
        map.lock("Hello");
        try {
            assertFalse(map.containsKey("Hello"));
        } finally {
            map.unlock("Hello");
        }
    }

    @Test
    public void testIssue290() throws Exception {
        String mapName = "testIssue290";
        Config config = new XmlConfigBuilder().build();
        MapConfig mapConfig = new MapConfig();
        mapConfig.setName(mapName);
        mapConfig.setTimeToLiveSeconds(1);
        config.addMapConfig(mapConfig);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        IMap<Object, Object> m1 = h1.getMap(mapName);
        m1.put(1, 1);
        assertEquals(1, m1.get(1));
        assertEquals(1, m1.get(1));
        Thread.sleep(1050);
        assertEquals(null, m1.get(1));
        m1.put(1, 1);
        assertEquals(1, m1.get(1));
    }
}
