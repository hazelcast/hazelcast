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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


public class HazelcastClientTest {
    com.hazelcast.core.IMap<Object, Object> realMap = Hazelcast.getMap("default");
    private HazelcastClient hClient;

    @Before
    public void init() {
        hClient = HazelcastClient.getHazelcastClient(new InetSocketAddress(Hazelcast.getCluster().getLocalMember().getInetAddress(),Hazelcast.getCluster().getLocalMember().getPort()));
        realMap.clear();
    }

    @After
    public void after(){
//    	Hazelcast.shutdownAll();
    }
    @Test
    public void putToTheMap() throws InterruptedException {
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
    public void getPuttedValueFromTheMap() {
        Map<String, String> clientMap = hClient.getMap("default");
        int size = realMap.size();
        clientMap.put("1", "Z");
        String value = clientMap.get("1");
        assertEquals("Z", value);
        assertEquals(size + 1, realMap.size());

    }


    @Test
    public void rollbackTransaction() {
        Transaction transaction = hClient.getTransaction();
        transaction.begin();
        Map<String, String> map = hClient.getMap("default");
        map.put("1", "A");
        assertEquals("A", map.get("1"));
        transaction.rollback();
        assertNull(map.get("1"));
    }

    @Test
    public void commitTransaction() {
        Transaction transaction = hClient.getTransaction();
        transaction.begin();
        Map<String, String> map = hClient.getMap("default");
        map.put("1", "A");
        assertEquals("A", map.get("1"));
        transaction.commit();
        assertEquals("A", map.get("1"));
    }

    @Test
    public void itertateOverMapEntries() {
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
    public void addListener() throws InterruptedException {
        IMap<String, String> map = hClient.getMap("default");
        realMap.clear();
        final CountDownLatch entryAddLatch = new CountDownLatch(1);
        final CountDownLatch entryUpdatedLatch = new CountDownLatch(1);
        final CountDownLatch entryRemovedLatch = new CountDownLatch(1);
        map.addEntryListener(new EntryListener() {
            public void entryAdded(EntryEvent event) {
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
    public void addListenerWithTwoMemberClusterAndKillOne() throws InterruptedException, IOException {
    	HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
    	HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
    	
    	Map<Integer, HazelcastInstance> memberMap = new HashMap<Integer, HazelcastInstance>();
    	memberMap.put(h1.getCluster().getLocalMember().getPort(), h1);
    	memberMap.put(h2.getCluster().getLocalMember().getPort(), h2);

    	HazelcastClient client = HazelcastClient.getHazelcastClient(new InetSocketAddress(h1.getCluster().getLocalMember().getInetAddress(),h1.getCluster().getLocalMember().getPort()),
    																new InetSocketAddress(h2.getCluster().getLocalMember().getInetAddress(),h2.getCluster().getLocalMember().getPort()));
        IMap<String, String> map = client.getMap("default");
        final Map<String, Boolean> m = new HashMap<String, Boolean>();
        final CountDownLatch entryAddLatch = new CountDownLatch(2);
        final CountDownLatch entryUpdatedLatch = new CountDownLatch(2);
        final CountDownLatch entryRemovedLatch = new CountDownLatch(2);
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
        memberMap.remove(client.getConnectionManager().getConnection().getAddress().getPort()).shutdown();
		map.put("hello", "world");
        System.out.println("PUT");
        map.put("hello", "new world");
        map.remove("hello");
        assertTrue(entryAddLatch.await(10, TimeUnit.MILLISECONDS));
        assertTrue(entryUpdatedLatch.await(10, TimeUnit.MILLISECONDS));
//        assertTrue(entryRemovedLatch.await(10, TimeUnit.MILLISECONDS));
        for (Iterator<Integer> iterator = memberMap.keySet().iterator(); iterator.hasNext();) {
			 memberMap.get(iterator.next()).shutdown();
			
		}
    }
    
    @Test
    public void put10000RecordsWith1ClusterMember(){
    	HazelcastClient client = HazelcastClient.getHazelcastClient(new InetSocketAddress(Hazelcast.getCluster().getLocalMember().getInetAddress(),Hazelcast.getCluster().getLocalMember().getPort()));
    	Map<String, String> map = client.getMap("default");
    	for (int i = 0; i < 10000; i++) {
			map.put("a", "b");
		}
    	assertTrue(true);
    }
    
	@Test
	public void continuePutAndGetIfOneOfConnectedClusterMemberFails() throws InterruptedException, IOException{
		HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
		HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
		HazelcastInstance h3 = Hazelcast.newHazelcastInstance(null);
		
		Map<Integer, HazelcastInstance> memberMap = new HashMap<Integer, HazelcastInstance>();
    	memberMap.put(h1.getCluster().getLocalMember().getPort(), h1);
    	memberMap.put(h2.getCluster().getLocalMember().getPort(), h2);
    	memberMap.put(h3.getCluster().getLocalMember().getPort(), h3);
		
		HazelcastClient client = HazelcastClient.getHazelcastClient(
				new InetSocketAddress(h1.getCluster().getLocalMember().getInetAddress(),h1.getCluster().getLocalMember().getPort()),
				new InetSocketAddress(h2.getCluster().getLocalMember().getInetAddress(),h2.getCluster().getLocalMember().getPort()),
				new InetSocketAddress(h3.getCluster().getLocalMember().getInetAddress(),h3.getCluster().getLocalMember().getPort()));
		
		Map<String, Integer> map = client.getMap("default");
		int counter = 0;
		while(counter<3){
			map.put("key",counter);
			assertEquals(counter, realMap.get("key"));
			assertEquals(counter, map.get("key"));
			memberMap.get(client.getConnectionManager().getConnection().getAddress().getPort()).shutdown();
			counter++;
		}
	}
	
	@Test(expected = RuntimeException.class)
	public void throwsRuntimeExceptionWhenNoMemberToConnect() throws InterruptedException, IOException{
		HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
		HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
		
		Map<Integer, HazelcastInstance> memberMap = new HashMap<Integer, HazelcastInstance>();
    	memberMap.put(h1.getCluster().getLocalMember().getPort(), h1);
    	memberMap.put(h2.getCluster().getLocalMember().getPort(), h2);
		
		HazelcastClient client = HazelcastClient.getHazelcastClient(
				new InetSocketAddress(h1.getCluster().getLocalMember().getInetAddress(),h1.getCluster().getLocalMember().getPort()),
				new InetSocketAddress(h2.getCluster().getLocalMember().getInetAddress(),h2.getCluster().getLocalMember().getPort()));
		Map<String, Integer> map = client.getMap("default");
		int counter = 0;
		while(counter<3){
			map.put("key",counter);
			assertEquals(counter, realMap.get("key"));
			assertEquals(counter, map.get("key"));
			memberMap.get(client.getConnectionManager().getConnection().getAddress().getPort()).shutdown();
			counter++;
		}
	}
}
