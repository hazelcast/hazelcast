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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.QueueConfig;

@RunWith(SpringJUnit4ClassRunner.class)

@ContextConfiguration(locations = { "fullcacheconfig-applicationContext-hazelcast.xml" })
public class TestFullApplicationContext {

	@Autowired
	Config config;
	
	
	@Test
	public void testConfig() {
		assertNotNull(config);
		
		MapConfig mapConfig = config.getMapConfig("testMap");
		assertNotNull(mapConfig);
		assertEquals("testMap", mapConfig.getName());
		assertEquals(2, mapConfig.getBackupCount());

		QueueConfig qConfig = config.getQueueConfig("testQ");
		assertNotNull(qConfig);
		assertEquals("testQ", qConfig.getName());
		assertEquals(1000, qConfig.getMaxSizePerJVM());

		
	}
	
}
