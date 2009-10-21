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
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Test;


import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


public class HazelcastClientTest{
    private HazelcastClient hClient;

    @After
    public void after() throws InterruptedException{
    	Hazelcast.shutdownAll();
    	if(hClient!=null){	hClient.shutdown(); }
    	Thread.sleep(500);
    }
    @Test
    public void putToTheMap() throws InterruptedException {
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
    	com.hazelcast.core.IMap<Object, Object> realMap = h.getMap("default");
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
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	com.hazelcast.core.IMap<Object, Object> realMap = h.getMap("default");
    	hClient = getHazelcastClient(h);
        Map<String, String> clientMap = hClient.getMap("default");
        int size = realMap.size();
        clientMap.put("1", "Z");
        String value = clientMap.get("1");
        assertEquals("Z", value);
        assertEquals(size + 1, realMap.size());

    }


    @Test
    public void rollbackTransaction() {
      	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
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
      	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
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
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
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
    public void addListener() throws InterruptedException, IOException {
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	com.hazelcast.core.IMap<Object, Object> realMap = h.getMap("default");
    	hClient = getHazelcastClient(h);
        final IMap<String, String> map = hClient.getMap("default");
        realMap.clear();
        assertEquals(0, realMap.size());
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
        assertNull(realMap.get("hello"));
        map.put("hello", "world");
        map.put("hello", "new world");
        assertEquals("new world", map.get("hello"));
        realMap.remove("hello");
        assertTrue(entryAddLatch.await(10, TimeUnit.MILLISECONDS));
        assertTrue(entryUpdatedLatch.await(10, TimeUnit.MILLISECONDS));
        assertTrue(entryRemovedLatch.await(10, TimeUnit.MILLISECONDS));

    }
    
   
    
    @Test
    public void put1000RecordsWith1ClusterMember(){
      	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
    	Map<String, String> map = hClient.getMap("default");
    	long time = System.currentTimeMillis();
    	for (int i = 0; i < 1000; i++) {
			map.put("a", "b");
		}
    	assertTrue(true);
    	System.out.println(System.currentTimeMillis()-time);
    }
    
    @Test
    public void testSuperClient(){
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	Config c2 = new XmlConfigBuilder().build();
        c2.setPortAutoIncrement(false);
        c2.setPort(5710);
        // make sure to super client = true
        c2.setSuperClient(true);
        HazelcastInstance hSuper = Hazelcast.newHazelcastInstance(c2);
        Map map = hSuper.getMap("map");
        long time = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
			map.put("a", "b");
		}
        System.out.println(System.currentTimeMillis()-time);

    }
    
	public static void printThreads() {
		System.out.println("All running threads");
		Thread[] threads = getAllThreads();
		for (int i = 0; i < threads.length; i++) {
			Thread t = threads[i];
			System.out.println(t.getName());
		}
		System.out.println("End of running threads");
	}
	static ThreadGroup rootThreadGroup = null;

	public static ThreadGroup getRootThreadGroup( ) {
	    if ( rootThreadGroup != null )
	        return rootThreadGroup;
	    ThreadGroup tg = Thread.currentThread( ).getThreadGroup( );
	    ThreadGroup ptg;
	    while ( (ptg = tg.getParent( )) != null )
	        tg = ptg;
	    return tg;
	}
	
	public static Thread[] getAllThreads( ) {
	    final ThreadGroup root = getRootThreadGroup( );
	    final ThreadMXBean thbean = ManagementFactory.getThreadMXBean( );
	    int nAlloc = thbean.getThreadCount( );
	    int n = 0;
	    Thread[] threads;
	    do {
	        nAlloc *= 2;
	        threads = new Thread[ nAlloc ];
	        n = root.enumerate( threads, true );
	    } while ( n == nAlloc );
	    return java.util.Arrays.copyOf( threads, n );
	}
	
	public static HazelcastClient getHazelcastClient(HazelcastInstance ... h) {
		InetSocketAddress[] addresses = new InetSocketAddress[h.length];
		for (int i = 0; i < h.length; i++) {
			addresses[i] = new InetSocketAddress(h[i].getCluster().getLocalMember().getInetAddress(),h[i].getCluster().getLocalMember().getPort());
		}
		HazelcastClient client = HazelcastClient.getHazelcastClient(addresses);
		return client;
	}
}
