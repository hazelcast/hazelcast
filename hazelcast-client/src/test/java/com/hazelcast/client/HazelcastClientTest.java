/*
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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

package com.hazelcast.client;
import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

//import com.hazelcast.core.Hazelcast;
//import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.client.core.Transaction;




public class HazelcastClientTest {
//	Map<String, String> realMap = Hazelcast.getMap("default");
	private HazelcastClient hClient;
	@Before public void init(){
		ClusterConfig config = new ClusterConfig();
		config.setHost("192.168.70.1");
		config.setPort(5701);
		hClient = HazelcastClient.getHazelcastClient(config);
//		realMap.clear();
	}
	
	@Test
	public void shouldBeAbleToPutToTheMap() throws InterruptedException{
		Map<String, String> clientMap = hClient.getMap("default");
//		assertEquals(0, realMap.size());
		String result = clientMap.put("1", "CBDEF");
		System.out.println("Result" + result);
		assertNull(result);
//		Object oldValue = realMap.get("1");
//		assertEquals("C", oldValue);
//		assertEquals(1, realMap.size());
		result = clientMap.put("1","B");
		System.out.println("DONE" + result);
		assertEquals("CBDEF", result);
	}
	
	@Test
	public void shouldGetPuttedValueFromTheMap(){
		Map<String, String> clientMap = hClient.getMap("default");
//		int size = realMap.size();
		clientMap.put("1", "Z");
		String value = clientMap.get("1");
		assertEquals("Z", value);
//		assertEquals(size+1, realMap.size());
		
	}
	
	public void shouldGetPuttedValueFromTheMapWhenThreeClusterMemberIsUp(){
//		HazelcastInstance i1 = Hazelcast.newHazelcastInstance(null);
//		i1.getMap("default");
//		HazelcastInstance i2 = Hazelcast.newHazelcastInstance(null);
//		i2.getMap("default");
		Map<String, String> clientMap = hClient.getMap("default");
//		int size = realMap.size();
		clientMap.put("1", "Z");
		String value = clientMap.get("1");
		assertEquals("Z", value);
//		assertEquals(size+1, realMap.size());
	}
	
	@Test
	public void shouldBeAbleToRollbackTransaction(){
		Transaction transaction = hClient.getTransaction();
		transaction.begin();
		Map<String, String> map = hClient.getMap("default");
		map.put("1", "A");
		assertEquals("A", map.get("1"));
		transaction.rollback();
		assertNull(map.get("1"));
	}
	@Test
	public void shouldBeAbleToCommitTransaction(){
		Transaction transaction = hClient.getTransaction();
		transaction.begin();
		Map<String, String> map = hClient.getMap("default");
		map.put("1", "A");
		assertEquals("A", map.get("1"));
		transaction.commit();
		assertEquals("A", map.get("1"));
	}
	@Test
	public void shouldItertateOverMapEntries(){
		Map<String, String> map = hClient.getMap("default");
		map.put("1", "A");
		Set<String> keySet = map.keySet();
		assertEquals(1,keySet.size());
		for (String string : keySet) {
			assertEquals("1", string);
		}
	}  
}
