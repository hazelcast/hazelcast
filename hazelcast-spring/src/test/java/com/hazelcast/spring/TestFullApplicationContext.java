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

import java.util.Properties;

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
import com.hazelcast.impl.GroupProperties;

@RunWith(SpringJUnit4ClassRunner.class)

@ContextConfiguration(locations = { "fullcacheconfig-applicationContext-hazelcast.xml" })
public class TestFullApplicationContext {

	@Autowired
	private Config config;
	
	
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
		assertEquals("group", groupConfig.getName());
		assertEquals("group-pass", groupConfig.getPassword());
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
		assertFalse(networkConfig.getJoin().getMulticastConfig().isEnabled());
		TcpIpConfig tcp = networkConfig.getJoin().getTcpIpConfig();
		assertNotNull(tcp);
		assertTrue(tcp.isEnabled());
		assertTrue(networkConfig.getSymmetricEncryptionConfig().isEnabled());
		
	}

	@Test
    public void testProperties() {
        final Properties properties = config.getProperties();
        assertNotNull(properties);
        assertEquals("5", properties.get(GroupProperties.PROP_MERGE_FIRST_RUN_DELAY_SECONDS));
        assertEquals("5", properties.get(GroupProperties.PROP_MERGE_NEXT_RUN_DELAY_SECONDS));
    }

}
