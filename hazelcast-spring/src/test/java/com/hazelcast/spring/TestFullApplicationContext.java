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
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
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

	@Autowired
	private Config config;
	
	@Autowired
	private HazelcastInstance instance;
	
	@AfterClass
	@BeforeClass
	public static void shutdown(){
	    Hazelcast.shutdownAll();
	}
	
	@Test
	public void testMapConfig() {
		assertNotNull(config);
		
		MapConfig mapConfig = config.getMapConfig("testMap");
		assertNotNull(mapConfig);
		assertEquals("testMap", mapConfig.getName());
		assertEquals(2, mapConfig.getBackupCount());

	}
	
	@Test
	public void testQueueConfig() {
		QueueConfig qConfig = config.getQueueConfig("testQ");
		assertNotNull(qConfig);
		assertEquals("testQ", qConfig.getName());
		assertEquals(1000, qConfig.getMaxSizePerJVM());
		
	}
	
	@Test
	public void testGroupConfig() {
		GroupConfig groupConfig = config.getGroupConfig();
		assertNotNull(groupConfig);
		assertEquals("spring-group", groupConfig.getName());
		assertEquals("spring-group-pass", groupConfig.getPassword());
	}

	@Test
	public void testExecutorCnnfig(){
		ExecutorConfig execConfig = config.getExecutorConfig("testExec");
		assertNotNull(execConfig);
		assertEquals(2, execConfig.getCorePoolSize());
		assertEquals(32, execConfig.getMaxPoolSize());
		assertEquals(30, execConfig.getKeepAliveSeconds());
	}
	
	@Test
	public void testNetworkConfig() {
		NetworkConfig networkConfig = config.getNetworkConfig();
		assertNotNull(networkConfig);
		assertEquals(5800, config.getPort());
		assertFalse(config.isPortAutoIncrement());
		assertFalse(networkConfig.getJoin().getMulticastConfig().isEnabled());
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
