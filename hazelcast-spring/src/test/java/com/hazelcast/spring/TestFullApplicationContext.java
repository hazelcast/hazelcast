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

import static org.junit.Assert.*;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.hazelcast.config.Config;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.impl.GroupProperties;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "fullcacheconfig-applicationContext-hazelcast.xml" })
public class TestFullApplicationContext {

	private Config config;
	
	@Autowired
    private HazelcastInstance instance;
	
	@BeforeClass
	@AfterClass
	public static void start(){
	    Hazelcast.shutdownAll();
	}
	
	@Before
	public void before(){
	    config = instance.getConfig();
	}
	
	@Test
	public void testMapConfig() {
		assertNotNull(config);
		
		assertEquals(2, config.getMapConfigs().size());
		
		MapConfig testMapConfig = config.getMapConfig("testMap");
		assertNotNull(testMapConfig);
        assertEquals("testMap", testMapConfig.getName());
        assertEquals(2, testMapConfig.getBackupCount());
        assertEquals("NONE", testMapConfig.getEvictionPolicy());
        assertEquals(0, testMapConfig.getMaxSize());
        assertEquals(30, testMapConfig.getEvictionPercentage());
        assertEquals(0, testMapConfig.getTimeToLiveSeconds());
        assertEquals("hz.ADD_NEW_ENTRY", testMapConfig.getMergePolicy());
        
		MapConfig simpleMapConfig = config.getMapConfig("simpleMap");
        assertNotNull(simpleMapConfig);
        assertEquals("simpleMap", simpleMapConfig.getName());
        assertEquals(3, simpleMapConfig.getBackupCount());
        assertEquals("LRU", simpleMapConfig.getEvictionPolicy());
        assertEquals(10, simpleMapConfig.getMaxSize());
        assertEquals(50, simpleMapConfig.getEvictionPercentage());
        assertEquals(1, simpleMapConfig.getTimeToLiveSeconds());
        assertEquals("hz.LATEST_UPDATE", simpleMapConfig.getMergePolicy());
	}
	
	@Test
	public void testQueueConfig() {
		QueueConfig testQConfig = config.getQueueConfig("testQ");
		assertNotNull(testQConfig);
		assertEquals("testQ", testQConfig.getName());
		assertEquals(1000, testQConfig.getMaxSizePerJVM());
		assertEquals(0, testQConfig.getTimeToLiveSeconds());
		
		QueueConfig qConfig = config.getQueueConfig("q");
        assertNotNull(qConfig);
        assertEquals("q", qConfig.getName());
        assertEquals(2500, qConfig.getMaxSizePerJVM());
        assertEquals(1, qConfig.getTimeToLiveSeconds());
	}
	
	@Test
	public void testGroupConfig() {
		GroupConfig groupConfig = config.getGroupConfig();
		assertNotNull(groupConfig);
		assertEquals("spring-group", groupConfig.getName());
		assertEquals("spring-group-pass", groupConfig.getPassword());
	}

	@Test
	public void testExecutorConfig(){
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
	}

	@Test
    public void testProperties() {
        final Properties properties = config.getProperties();
        assertNotNull(properties);
        assertEquals("5", properties.get(GroupProperties.PROP_MERGE_FIRST_RUN_DELAY_SECONDS));
        assertEquals("5", properties.get(GroupProperties.PROP_MERGE_NEXT_RUN_DELAY_SECONDS));
    }
	
	@Test
	public void testInstance(){
	    assertNotNull(instance);
	    final Set<Member> members = instance.getCluster().getMembers();
	    assertEquals(1, members.size());
	    final Member member = members.iterator().next();
	    final InetSocketAddress inetSocketAddress = member.getInetSocketAddress();
	    assertEquals(5800, inetSocketAddress.getPort());
	}

}
