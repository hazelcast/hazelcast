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

package com.hazelcast.spring;

import com.hazelcast.config.*;
import com.hazelcast.core.*;
import com.hazelcast.impl.GroupProperties;
import com.hazelcast.impl.wan.WanReplicationEndpoint;
import com.hazelcast.merge.MergePolicy;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static org.junit.Assert.*;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"fullcacheconfig-applicationContext-hazelcast.xml"})
public class TestFullApplicationContext {

    private Config config;

    @Resource(name = "instance")
    private HazelcastInstance instance;

    @Resource(name = "map1")
    private IMap<Object, Object> map1;

    @Resource(name = "map2")
    private IMap<Object, Object> map2;

    @Resource(name = "multiMap")
    private MultiMap multiMap;

    @Resource(name = "queue")
    private IQueue queue;

    @Resource(name = "topic")
    private ITopic topic;

    @Resource(name = "set")
    private ISet set;

    @Resource(name = "list")
    private IList list;

    @Resource(name = "executorService")
    private ExecutorService executorService;

    @Resource(name = "idGenerator")
    private IdGenerator idGenerator;

    @Resource(name = "atomicLong")
    private AtomicNumber atomicLong;

    @Resource(name="countDownLatch")
    private ICountDownLatch countDownLatch;

    @Resource(name="semaphore")
    private ISemaphore semaphore;

    @Resource(name = "dummyMapStore")
    private MapStore dummyMapStore;
    
    @Autowired
    private WanReplicationEndpoint wanReplication;
    
    @Autowired
    private MergePolicy dummyMergePolicy;

    @BeforeClass
    @AfterClass
    public static void start() {
        Hazelcast.shutdownAll();
    }

    @Before
    public void before() {
        config = instance.getConfig();
    }

    @Test
    public void testMapConfig() {
        assertNotNull(config);
        System.out.println(config.getMapConfigs().keySet());
        assertEquals(4, config.getMapConfigs().size());
        MapConfig testMapConfig = config.getMapConfig("testMap");
        assertNotNull(testMapConfig);
        assertEquals("testMap", testMapConfig.getName());
        assertEquals(2, testMapConfig.getBackupCount());
        assertEquals("NONE", testMapConfig.getEvictionPolicy());
        assertEquals(Integer.MAX_VALUE, testMapConfig.getMaxSizeConfig().getSize());
        assertEquals(30, testMapConfig.getEvictionPercentage());
        assertEquals(0, testMapConfig.getTimeToLiveSeconds());
        assertEquals("hz.ADD_NEW_ENTRY", testMapConfig.getMergePolicy());
        assertTrue(testMapConfig.isReadBackupData());
        // Test that the testMapConfig has a mapStoreConfig and it is correct
        MapStoreConfig testMapStoreConfig = testMapConfig.getMapStoreConfig();
        assertNotNull(testMapStoreConfig);
        assertEquals("com.hazelcast.spring.DummyStore", testMapStoreConfig.getClassName());
        assertTrue(testMapStoreConfig.isEnabled());
        assertEquals(0, testMapStoreConfig.getWriteDelaySeconds());
        // Test that the testMapConfig has a nearCacheConfig and it is correct
        NearCacheConfig testNearCacheConfig = testMapConfig.getNearCacheConfig();
        assertNotNull(testNearCacheConfig);
        assertEquals(0, testNearCacheConfig.getTimeToLiveSeconds());
        assertEquals(60, testNearCacheConfig.getMaxIdleSeconds());
        assertEquals("LRU", testNearCacheConfig.getEvictionPolicy());
        assertEquals(5000, testNearCacheConfig.getMaxSize());
        assertTrue(testNearCacheConfig.isInvalidateOnChange());
        // Test that the testMapConfig2's mapStoreConfig implementation
        MapConfig testMapConfig2 = config.getMapConfig("testMap2");
        assertNotNull(testMapConfig2.getMapStoreConfig().getImplementation());
        assertEquals(dummyMapStore, testMapConfig2.getMapStoreConfig().getImplementation());
        assertEquals("testWan", testMapConfig2.getWanReplicationRef().getName());
        assertEquals("hz.ADD_NEW_ENTRY", testMapConfig2.getWanReplicationRef().getMergePolicy());
        MapConfig simpleMapConfig = config.getMapConfig("simpleMap");
        assertNotNull(simpleMapConfig);
        assertEquals("simpleMap", simpleMapConfig.getName());
        assertEquals(3, simpleMapConfig.getBackupCount());
        assertEquals("LRU", simpleMapConfig.getEvictionPolicy());
        assertEquals(10, simpleMapConfig.getMaxSizeConfig().getSize());
        assertEquals(50, simpleMapConfig.getEvictionPercentage());
        assertEquals(1, simpleMapConfig.getTimeToLiveSeconds());
        assertEquals("hz.LATEST_UPDATE", simpleMapConfig.getMergePolicy());
        // Test that the simpleMapConfig does NOT have a mapStoreConfig
        assertNull(simpleMapConfig.getMapStoreConfig());
        // Test that the simpleMapConfig does NOT have a nearCacheConfig
        assertNull(simpleMapConfig.getNearCacheConfig());
    }

    @Test
    public void testQueueConfig() {
        QueueConfig testQConfig = config.getQueueConfig("testQ");
        assertNotNull(testQConfig);
        assertEquals("testQ", testQConfig.getName());
        assertEquals(1000, testQConfig.getMaxSizePerJVM());
        QueueConfig qConfig = config.getQueueConfig("q");
        assertNotNull(qConfig);
        assertEquals("q", qConfig.getName());
        assertEquals(2500, qConfig.getMaxSizePerJVM());
    }

    @Test
    public void testGroupConfig() {
        GroupConfig groupConfig = config.getGroupConfig();
        assertNotNull(groupConfig);
        assertEquals("spring-group", groupConfig.getName());
        assertEquals("spring-group-pass", groupConfig.getPassword());
    }

    @Test
    public void testExecutorConfig() {
        ExecutorConfig testExecConfig = config.getExecutorConfig("testExec");
        assertNotNull(testExecConfig);
        assertEquals("testExec", testExecConfig.getName());
        assertEquals(2, testExecConfig.getCorePoolSize());
        assertEquals(32, testExecConfig.getMaxPoolSize());
        assertEquals(30, testExecConfig.getKeepAliveSeconds());
        ExecutorConfig testExec2Config = config.getExecutorConfig("testExec2");
        assertNotNull(testExec2Config);
        assertEquals("testExec2", testExec2Config.getName());
        assertEquals(5, testExec2Config.getCorePoolSize());
        assertEquals(10, testExec2Config.getMaxPoolSize());
        assertEquals(20, testExec2Config.getKeepAliveSeconds());
    }

    @Test
    public void testNetworkConfig() {
        NetworkConfig networkConfig = config.getNetworkConfig();
        assertNotNull(networkConfig);
        assertEquals(5800, config.getPort());
        assertFalse(config.isPortAutoIncrement());
        assertFalse(networkConfig.getJoin().getMulticastConfig().isEnabled());
        assertFalse(networkConfig.getInterfaces().isEnabled());
        assertEquals(1, networkConfig.getInterfaces().getInterfaces().size());
        assertEquals("10.10.1.*", networkConfig.getInterfaces().getInterfaces().iterator().next());
        TcpIpConfig tcp = networkConfig.getJoin().getTcpIpConfig();
        assertNotNull(tcp);
        assertTrue(tcp.isEnabled());
        assertTrue(networkConfig.getSymmetricEncryptionConfig().isEnabled());
        final List<String> members = tcp.getMembers();
        assertEquals(members.toString(), 2, members.size());
        assertEquals("127.0.0.1:5800", members.get(0));
        assertEquals("127.0.0.1:5801", members.get(1));
        assertEquals("127.0.0.1:5800", tcp.getRequiredMember());
    }

    @Test
    public void testSemaphoreConfig() {
        SemaphoreConfig testSemaphoreConfig = config.getSemaphoreConfig("testSemaphore");
        assertNotNull(testSemaphoreConfig);
        assertEquals(testSemaphoreConfig.getInitialPermits(), 0);
        assertEquals(testSemaphoreConfig.isFactoryEnabled(), false);
        assertNull(testSemaphoreConfig.getFactoryClassName());
    }

    @Test
    public void testProperties() {
        final Properties properties = config.getProperties();
        assertNotNull(properties);
        assertEquals("5", properties.get(GroupProperties.PROP_MERGE_FIRST_RUN_DELAY_SECONDS));
        assertEquals("5", properties.get(GroupProperties.PROP_MERGE_NEXT_RUN_DELAY_SECONDS));
        final Config config2 = instance.getConfig();
        final Properties properties2 = config2.getProperties();
        assertNotNull(properties2);
        assertEquals("5", properties2.get(GroupProperties.PROP_MERGE_FIRST_RUN_DELAY_SECONDS));
        assertEquals("5", properties2.get(GroupProperties.PROP_MERGE_NEXT_RUN_DELAY_SECONDS));
    }

    @Test
    public void testInstance() {
        assertNotNull(instance);
        final Set<Member> members = instance.getCluster().getMembers();
        assertEquals(1, members.size());
        final Member member = members.iterator().next();
        final InetSocketAddress inetSocketAddress = member.getInetSocketAddress();
        assertEquals(5800, inetSocketAddress.getPort());
    }

    @Test
    public void testHazelcastInstances() {
        assertNotNull(map1);
        assertNotNull(map2);
        assertNotNull(multiMap);
        assertNotNull(queue);
        assertNotNull(topic);
        assertNotNull(set);
        assertNotNull(list);
        assertNotNull(executorService);
        assertNotNull(idGenerator);
        assertNotNull(atomicLong);
        assertNotNull(countDownLatch);
        assertNotNull(semaphore);
        assertEquals("map1", map1.getName());
        assertEquals("map2", map2.getName());
        assertEquals("multiMap", multiMap.getName());
        assertEquals("queue", queue.getName());
        assertEquals("topic", topic.getName());
        assertEquals("set", set.getName());
        assertEquals("list", list.getName());
        assertEquals("idGenerator", idGenerator.getName());
        assertEquals("atomicLong", atomicLong.getName());
        assertEquals("countDownLatch", countDownLatch.getName());
        assertEquals("semaphore", semaphore.getName());
    }
    
    @Test
    public void testWanReplicationConfig() {
    	WanReplicationConfig wcfg = config.getWanReplicationConfig("testWan");
    	assertNotNull(wcfg);
    	assertEquals(2, wcfg.getTargetClusterConfigs().size());
    	
    	WanTargetClusterConfig targetCfg = wcfg.getTargetClusterConfigs().get(0);
    	assertNotNull(targetCfg);
    	assertEquals("tokyo", targetCfg.getGroupName());
    	assertEquals("tokyo-pass", targetCfg.getGroupPassword());
    	assertEquals("com.hazelcast.impl.wan.WanNoDelayReplication", targetCfg.getReplicationImpl());
    	assertEquals(2, targetCfg.getEndpoints().size());
    	assertEquals("10.2.1.1:5701", targetCfg.getEndpoints().get(0));
    	assertEquals("10.2.1.2:5701", targetCfg.getEndpoints().get(1));
    	
    	assertEquals(wanReplication, wcfg.getTargetClusterConfigs().get(1).getReplicationImplObject());
    }
    
    @Test
    public void testMapMergePolicyConfig() {
    	Map<String, MergePolicyConfig> merges = config.getMergePolicyConfigs();
    	assertEquals(1, merges.size());
    	MergePolicyConfig cfg = merges.values().iterator().next();
    	assertEquals("hz.MERGE_POLICY_TEST", cfg.getName());
    	assertEquals("com.hazelcast.spring.TestMapMergePolicy", cfg.getClassName());
    	assertEquals(dummyMergePolicy, cfg.getImplementation());
    }
}
