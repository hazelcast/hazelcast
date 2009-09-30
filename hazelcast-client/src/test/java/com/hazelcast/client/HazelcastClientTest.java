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

import com.hazelcast.client.core.EntryEvent;
import com.hazelcast.client.core.EntryListener;
import com.hazelcast.client.core.IMap;
import com.hazelcast.client.core.Transaction;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


public class HazelcastClientTest {
    com.hazelcast.core.IMap<Object, Object> realMap = Hazelcast.getMap("default");
    private HazelcastClient hClient;

    @Before
    public void init() {
        hClient = HazelcastClient.getHazelcastClient(new InetSocketAddress("192.168.70.1",5701));
        realMap.clear();
    }

//    @After
//    public void after(){
//    	Hazelcast.shutdownAll();
//    }
    @Test
    public void shouldBeAbleToPutToTheMap() throws InterruptedException {
        Map<String, String> clientMap = hClient.getMap("default");
        assertEquals(0, realMap.size());
        String result = clientMap.put("1", "CBDEF");
        System.out.println("Result" + result);
        assertNull(result);
        Object oldValue = realMap.get("1");
        assertEquals("CBDEF", oldValue);
        assertEquals(1, realMap.size());
        result = clientMap.put("1", "B");
        System.out.println("DONE" + result);
        assertEquals("CBDEF", result);
    }

    @Test
    public void shouldGetPuttedValueFromTheMap() {
        Map<String, String> clientMap = hClient.getMap("default");
        int size = realMap.size();
        clientMap.put("1", "Z");
        String value = clientMap.get("1");
        assertEquals("Z", value);
        assertEquals(size + 1, realMap.size());

    }


    @Test
    public void shouldBeAbleToRollbackTransaction() {
        Transaction transaction = hClient.getTransaction();
        transaction.begin();
        Map<String, String> map = hClient.getMap("default");
        map.put("1", "A");
        assertEquals("A", map.get("1"));
        transaction.rollback();
        assertNull(map.get("1"));
    }

    @Test
    public void shouldBeAbleToCommitTransaction() {
        Transaction transaction = hClient.getTransaction();
        transaction.begin();
        Map<String, String> map = hClient.getMap("default");
        map.put("1", "A");
        assertEquals("A", map.get("1"));
        transaction.commit();
        assertEquals("A", map.get("1"));
    }

    @Test
    public void shouldItertateOverMapEntries() {
        Map<String, String> map = hClient.getMap("default");
        map.put("1", "A");
        map.put("2", "B");
        map.put("3", "C");
        Set<String> keySet = map.keySet();
        assertEquals(3, keySet.size());
        Set<String> s = new HashSet<String>();
        for (String string : keySet) {
            s.add(string);
            assertTrue(Arrays.asList("1", "2", "3").contains(string));
        }
        assertEquals(3, s.size());
    }

    @Test
    public void shouldBeAbleToAddListener() throws InterruptedException {
        IMap<String, String> map = hClient.getMap("default");
        final Map<String, Boolean> m = new HashMap<String, Boolean>();
        final CountDownLatch entryAddLatch = new CountDownLatch(1);
        final CountDownLatch entryUpdatedLatch = new CountDownLatch(1);
        final CountDownLatch entryRemovedLatch = new CountDownLatch(1);
        map.addEntryListener(new EntryListener() {
            public void entryAdded(EntryEvent event) {
                m.put("entryAdded", true);
                assertEquals("hello", event.getKey());
                entryAddLatch.countDown();
            }

            public void entryRemoved(EntryEvent event) {
                entryRemovedLatch.countDown();
                assertEquals("hello", event.getKey());
                assertEquals("new world", event.getValue());
            }

            public void entryUpdated(EntryEvent event) {
                assertEquals("new world", event.getValue());
                assertEquals("hello", event.getKey());
                entryUpdatedLatch.countDown();
            }

            public void entryEvicted(EntryEvent event) {
                m.put("entryEvicted", true);
                entryRemoved(event);
            }
        }, true);
        map.put("hello", "world");
        System.out.println("PUT");
        map.put("hello", "new world");
        map.remove("hello");
        assertTrue(entryAddLatch.await(10, TimeUnit.MILLISECONDS));
        assertTrue(entryUpdatedLatch.await(10, TimeUnit.MILLISECONDS));
//        assertTrue(entryRemovedLatch.await(10, TimeUnit.MILLISECONDS));

    }

    @Test
    public void shouldBteAbleToAddListenerFromRemote() throws InterruptedException {
        IMap<String, String> map = hClient.getMap("default");
        final Map<String, Boolean> m = new HashMap<String, Boolean>();
        final CountDownLatch entryAddLatch = new CountDownLatch(1);
        final CountDownLatch entryUpdatedLatch = new CountDownLatch(1);
        final CountDownLatch entryRemovedLatch = new CountDownLatch(1);
        map.addEntryListener(new EntryListener() {
            public void entryAdded(EntryEvent event) {
            	entryAddLatch.countDown();
                System.out.println("Added" + event);
            }

            public void entryRemoved(EntryEvent event) {
            	entryRemovedLatch.countDown();
                System.out.println("Removed" + event);
            }

            public void entryUpdated(EntryEvent event) {
            	entryUpdatedLatch.countDown();
                System.out.println("Updated" + event);
            }

            public void entryEvicted(EntryEvent event) {
                System.out.println("Evicted" + event);
            }
        }, true);
    }
    
    @Test
    public void shouldPut10000RecordsWith1ClusterMember(){
    	HazelcastClient client = HazelcastClient.getHazelcastClient(new InetSocketAddress("192.168.70.1",5701));
    	Map map = client.getMap("default");
    	long ms = System.currentTimeMillis();
    	int counter= 0;
    	for (int i = 0; i < 10000; i++) {
			map.put("a", "b");
//			System.out.println(counter++);
		}
    	System.out.println(System.currentTimeMillis()-ms);
    	assertTrue(true);
    }
    
	@Test(expected = RuntimeException.class)
	public void shouldBeAbleToSwitchToAnotherMemberIfOneFails() throws InterruptedException{
		HazelcastInstance i1 = Hazelcast.newHazelcastInstance(null);
		HazelcastInstance i2 = Hazelcast.newHazelcastInstance(null);
		HazelcastInstance i3 = Hazelcast.newHazelcastInstance(null);
		List<HazelcastInstance> members = new ArrayList<HazelcastInstance>();
		members.add(i1);
		members.add(i2);
		members.add(i3);
		
		HazelcastClient client = HazelcastClient.getHazelcastClient(new InetSocketAddress("192.168.70.1",5704),new InetSocketAddress("192.168.70.1",5702),new InetSocketAddress("192.168.70.1",5703));
		Map map = client.getMap("default");
		int counter = 0;
		while(counter<4){
			map.put("key",counter);
			assertEquals(counter, realMap.get("key"));
			int id = client.getConnectionManager().getConnection().getAddress().getPort() - 5702;
			members.get(id).shutdown();
			Thread.sleep(2000);
			counter++;
		}
	}


}
