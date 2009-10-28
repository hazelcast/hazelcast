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

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapEntry;
import com.hazelcast.core.Transaction;
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Ignore;
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
    public void shutdownAll() throws InterruptedException{
    	Hazelcast.shutdownAll();
    	if(hClient!=null){	hClient.shutdown(); }
    	Thread.sleep(500);
    }
    @Test
    public void getMapName() throws InterruptedException {
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
    	IMap  map  = hClient.getMap("ABC");
        assertEquals("ABC", map.getName());
    }
    @Test
    public void lockMap() throws InterruptedException{
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
    	final IMap  map  = hClient.getMap("ABC");
    	final CountDownLatch latch = new CountDownLatch(1);
    	map.put("a", "b");
    	Thread.sleep(1000);
    	System.out.println("Getting the lock");
    	map.lock("a");
    	System.out.println("Got the lock");
    	new Thread(new Runnable(){
    		
    		public void run() {
    			map.lock("a");
    			System.out.println("Thread also get the lock");
    			latch.countDown();
    		}
    		
    	}).start();
    	Thread.sleep(100);
    	assertEquals(1, latch.getCount());
    	System.out.println("Unlocking the key");
    	map.unlock("a");
    	System.out.println("Unlocked the key");

    	assertTrue(latch.await(100000, TimeUnit.MILLISECONDS));
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
    public void removeFromMap(){
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
    	Map map = hClient.getMap("default");
    	assertNull(map.put("a", "b"));
    	assertEquals("b", map.get("a"));
    	assertEquals("b", map.remove("a"));
    	assertNull(map.remove("a"));
    	assertNull(map.get("a"));
    }
    @Test
    public void evictFromMap(){
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
    	IMap map = hClient.getMap("default");
    	assertNull(map.put("a", "b"));
    	assertEquals("b", map.get("a"));
    	assertTrue(map.evict("a"));
    	assertFalse(map.evict("a"));
    	assertNull(map.get("a"));
    }
    @Test
    public void getSize(){
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
    	IMap map = hClient.getMap("default");
    	assertEquals(0, map.size());
    	map.put("a", "b");
    	assertEquals(1, map.size());
    	for(int i = 0;i<100;i++){
    		map.put(String.valueOf(i), String.valueOf(i));
    	}
    	assertEquals(101, map.size());
    	map.remove("a");
    	assertEquals(100, map.size());
    	for(int i=0;i<50;i++){
    		map.remove(String.valueOf(i));
    	}
    	assertEquals(50, map.size());
    	for(int i=50;i<100;i++){
    		map.remove(String.valueOf(i));
    	}
    	assertEquals(0, map.size());
    }
    
    @Test
    public void getMapEntry(){
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
    	IMap map = hClient.getMap("default");
    	assertNull(map.put("a", "b"));
    	map.get("a");
    	map.get("a");
    	MapEntry<String, String> entry = map.getMapEntry("a");
    	assertEquals("a", entry.getKey());
    	assertEquals("b", entry.getValue());
    	assertEquals(2, entry.getHits());
    	assertEquals("b", entry.getValue());
    	assertEquals("b", entry.setValue("c"));
    	assertEquals("c", map.get("a"));
    	assertEquals("c", entry.getValue());
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
        map.addEntryListener(new EntryListener<String, String>() {
            public void entryAdded(EntryEvent<String, String> event) {
            	entryAddLatch.countDown();
                assertEquals("hello", event.getKey());
            }

            public void entryRemoved(EntryEvent<String, String> event) {
                entryRemovedLatch.countDown();
                assertEquals("hello", event.getKey());
                assertEquals("new world", event.getValue());
            }

            public void entryUpdated(EntryEvent<String, String> event) {
            	entryUpdatedLatch.countDown();
                assertEquals("new world", event.getValue());
                assertEquals("hello", event.getKey());
            }

            public void entryEvicted(EntryEvent<String, String> event) {
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
    public void putAndget100000RecordsWith1ClusterMember(){
      	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
    	
    	Map<String, String> map = hClient.getMap("default");
    	
    	putAndGet(map, 100000);
    }
    
    @Test
    @Ignore
    public void testSuperClient(){
    	Hazelcast.newHazelcastInstance(null);
    	
    	Config c2 = new XmlConfigBuilder().build();
        c2.setPortAutoIncrement(false);
        c2.setPort(5710);
        c2.setSuperClient(true);
        HazelcastInstance hSuper = Hazelcast.newHazelcastInstance(c2);
        
        Map<String,String> map = hSuper.getMap("default");
        
        putAndGet(map, 100000);

    }
    
	private void putAndGet(Map<String, String> map, int counter) {
		long beginTime = System.currentTimeMillis();
    	System.out.println("PUT");
    	for (int i = 0; i < counter; i++) {
    		if(i%10000 == 0){
    			System.out.println(i+": "+(System.currentTimeMillis()-beginTime)+" ms");
    		}
			map.put("key_"+i, String.valueOf(i));
		}
    	System.out.println(System.currentTimeMillis()-beginTime);

    	beginTime = System.currentTimeMillis();
    	System.out.println("GET");
    	for (int i = 0; i < counter; i++) {
    		if(i%10000 == 0){
    			System.out.println(i+": "+(System.currentTimeMillis()-beginTime)+" ms");
    		}
    		assertEquals(String.valueOf(i), map.get("key_"+i));
		}
    	
//    	assertEquals(String.valueOf(i), map.get("key_"+i));
    	System.out.println(System.currentTimeMillis()-beginTime);
	}
    

    
	public static void printThreads() {
		Thread[] threads = getAllThreads();
		for (int i = 0; i < threads.length; i++) {
			Thread t = threads[i];
			System.out.println(t.getName());
		}
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
	    return threads;
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
