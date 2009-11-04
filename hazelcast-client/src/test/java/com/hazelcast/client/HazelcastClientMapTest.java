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

import com.hazelcast.core.IMap;
import com.hazelcast.core.MapEntry;
import com.hazelcast.core.Transaction;
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.DataSerializable;
import com.hazelcast.query.EntryObject;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.QueryTest;
import com.hazelcast.query.QueryTest.Employee;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


public class HazelcastClientMapTest{
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
    	System.out.println("Got the lock " + Thread.currentThread().getId());
    	new Thread(new Runnable(){
    		public void run() {
    			map.lock("a");
    			System.out.println("Thread also get the lock " + Thread.currentThread().getId());
    			latch.countDown();
    		}
    	}).start();
    	Thread.sleep(100);
    	assertEquals(1, latch.getCount());
    	System.out.println("Unlocking the key");
    	map.unlock("a");
    	System.out.println("Unlocked the key");

    	assertTrue(latch.await(2000, TimeUnit.MILLISECONDS));
    }
    @Test
    public void addIndex(){
    	
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
    	IMap map  = hClient.getMap("employees");
    	int size = 1000;
    	for(int i=0;i<size;i++){
    		map.put(String.valueOf(i), new Employee("name"+i, i, true, 0 ));
    	}
    	EntryObject e = new PredicateBuilder().getEntryObject();
        Predicate predicate = e.get("age").equal(23);
        long begin = System.currentTimeMillis();
        Set<Entry<Object, Object>> set = map.entrySet(predicate);
        long timeWithoutIndex = System.currentTimeMillis() - begin; 
        assertEquals(1, set.size());
        assertEquals(size, map.size());
        
        map.destroy();
        
        map  = hClient.getMap("employees");
        map.addIndex("age", true);
        for(int i=0;i<size;i++){
    		map.put(String.valueOf(i), new Employee("name"+i, i, true, 0 ));
    	}
        begin = System.currentTimeMillis();
        set = map.entrySet(predicate);
        long timeWithIndex = System.currentTimeMillis() - begin; 
        assertEquals(1, set.size());
        assertEquals(size, map.size());
        
        assertTrue(timeWithoutIndex > 2*timeWithIndex);
        
        
        
        

        //    	map.addIndex("age", true);
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
    	hClient = getHazelcastClient(h);
    	com.hazelcast.core.IMap<Object, Object> realMap = h.getMap("default");
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
    
    public class Customer implements DataSerializable{
		private String name;
		private int age;
		
		public Customer(String name, int age) {
			this.name = name;
			this.age = age;
		}
		public void setName(String name) {
			this.name = name;
		}
		public String getName() {
			return name;
		}
		public void setAge(int age) {
			this.age = age;
		}
		public int getAge() {
			return age;
		}
		public void readData(DataInput in) throws IOException {
			this.age = in.readInt();
			int size = in.readInt();
			byte[] bytes = new byte[size];
			in.readFully(bytes);
			this.name = new String(bytes);
		}
		public void writeData(DataOutput out) throws IOException {
			out.writeInt(age);
			byte[] bytes = name.getBytes();
			out.writeInt(bytes.length);
			out.write(bytes);
			
		}
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
    public void itertateOverMapKeys() {
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
    public void itertateOverMapEntries() {
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
        IMap<String, String> map = hClient.getMap("default");
        map.put("1", "A");
        map.put("2", "B");
        map.put("3", "C");
        Set<Entry<String, String>> entrySet = map.entrySet();
        assertEquals(3, entrySet.size());
        Set<String> keySet = map.keySet();
        for (Entry<String,String> entry : entrySet) {
        	assertTrue(keySet.contains(entry.getKey()));
        	assertEquals(entry.getValue(), map.get(entry.getKey()));
        }
        for (String key : keySet) {
        	MapEntry  mapEntry = map.getMapEntry(key);
        	assertEquals(2, mapEntry.getHits());
        }
    }
    @Test
    public void tryLock() throws InterruptedException{
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
        final IMap<String, String> map = hClient.getMap("default");
        final CountDownLatch latch = new CountDownLatch(3);
        map.put("1", "A");
        map.lock("1");
        new Thread(new Runnable(){
			public void run() {
				if(!map.tryLock("1", 100, TimeUnit.MILLISECONDS)){
					latch.countDown();
				}
				if(!map.tryLock("1")){
					latch.countDown();
				}
				if(map.tryLock("2")){
					latch.countDown();
				}
			}
        }).start();
    	assertTrue(latch.await(100, TimeUnit.MILLISECONDS));
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
        CountDownLatchEntryListener<String, String> listener = new CountDownLatchEntryListener<String, String>(entryAddLatch, entryUpdatedLatch, entryRemovedLatch);
        map.addEntryListener(listener, true);
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
    public void addTwoListener1ToMapOtherToKey() throws InterruptedException, IOException {
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
        final IMap<String, String> map = hClient.getMap("default");
        final CountDownLatch entryAddLatch = new CountDownLatch(5);
        final CountDownLatch entryUpdatedLatch = new CountDownLatch(5);
        final CountDownLatch entryRemovedLatch = new CountDownLatch(5);
        CountDownLatchEntryListener<String, String> listener1 = new CountDownLatchEntryListener<String, String>(entryAddLatch, entryUpdatedLatch, entryRemovedLatch);
        CountDownLatchEntryListener<String, String> listener2 = new CountDownLatchEntryListener<String, String>(entryAddLatch, entryUpdatedLatch, entryRemovedLatch);
        map.addEntryListener(listener1, true);
        map.addEntryListener(listener2, "hello", true);
        map.put("hello", "world");
        map.put("hello", "new world");
        map.remove("hello");
        Thread.sleep(100);
        
        assertEquals(3, entryAddLatch.getCount());
        assertEquals(3, entryRemovedLatch.getCount());
        assertEquals(3, entryUpdatedLatch.getCount());
    }
    @Test
    public void addTwoListener1stToKeyOtherToMap() throws InterruptedException, IOException {
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
        final IMap<String, String> map = hClient.getMap("default");
        final CountDownLatch entryAddLatch = new CountDownLatch(5);
        final CountDownLatch entryUpdatedLatch = new CountDownLatch(5);
        final CountDownLatch entryRemovedLatch = new CountDownLatch(5);
        CountDownLatchEntryListener<String, String> listener1 = new CountDownLatchEntryListener<String, String>(entryAddLatch, entryUpdatedLatch, entryRemovedLatch);
        CountDownLatchEntryListener<String, String> listener2 = new CountDownLatchEntryListener<String, String>(entryAddLatch, entryUpdatedLatch, entryRemovedLatch);
        map.addEntryListener(listener2, "hello", true);
        map.addEntryListener(listener1, true);
        map.put("hello", "world");
        map.put("hello", "new world");
        map.remove("hello");
        Thread.sleep(100);
        
        assertEquals(3, entryAddLatch.getCount());
        assertEquals(3, entryRemovedLatch.getCount());
        assertEquals(3, entryUpdatedLatch.getCount());
    }
    @Test
    public void removeListener() throws InterruptedException, IOException {
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
        final IMap<String, String> map = hClient.getMap("default");
        final CountDownLatch entryAddLatch = new CountDownLatch(5);
        final CountDownLatch entryUpdatedLatch = new CountDownLatch(5);
        final CountDownLatch entryRemovedLatch = new CountDownLatch(5);
        CountDownLatchEntryListener<String, String> listener1 = new CountDownLatchEntryListener<String, String>(entryAddLatch, entryUpdatedLatch, entryRemovedLatch);
        CountDownLatchEntryListener<String, String> listener2 = new CountDownLatchEntryListener<String, String>(entryAddLatch, entryUpdatedLatch, entryRemovedLatch);
        map.addEntryListener(listener1, true);
        map.put("hello", "world");
        map.put("hello", "new world");
        map.remove("hello");
        Thread.sleep(100);
        
        assertEquals(4, entryAddLatch.getCount());
        assertEquals(4, entryRemovedLatch.getCount());
        assertEquals(4, entryUpdatedLatch.getCount());
        
        map.removeEntryListener(listener1);
        
        map.put("hello", "world");
        map.put("hello", "new world");
        map.remove("hello");
  
        Thread.sleep(100);
        assertEquals(4, entryAddLatch.getCount());
        assertEquals(4, entryRemovedLatch.getCount());
        assertEquals(4, entryUpdatedLatch.getCount());
    }
    @Test
    public void putIfAbsent(){
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
        IMap<String, String> map = hClient.getMap("default");
        String result = map.put("1", "CBDEF");
        assertNull(result);
        assertNull(map.putIfAbsent("2", "C"));
        assertEquals("C", map.putIfAbsent("2", "D"));
    }
    
    @Test
    public void remove(){
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
        IMap<String, String> map = hClient.getMap("default");
        String result = map.put("1", "CBDEF");
        assertNull(result);
        assertFalse(map.remove("1", "CBD"));
        assertEquals("CBDEF", map.get("1"));
        assertTrue(map.remove("1", "CBDEF"));
    }
    
    @Test
    public void replace(){
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
        IMap<String, String> map = hClient.getMap("default");
        String result = map.put("1", "CBDEF");
        assertNull(result);
        assertEquals("CBDEF", map.replace("1", "CBD"));
        assertNull(map.replace("2", "CBD"));
        assertFalse(map.replace("2","CBD","ABC"));
        assertTrue(map.replace("1","CBD","XX"));
    }
    @Test
    public void clear(){
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
        IMap<Integer, Integer> map = hClient.getMap("default");
        for(int i=0;i<100;i++){
        	assertNull(map.put(i, i));
        	assertEquals(i, map.get(i));
        }
        map.clear();
        for(int i=0;i<100;i++){
        	assertNull(map.get(i));
        }
    }
    @Test
    public void destroy(){
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
        IMap<Integer, Integer> map = hClient.getMap("default");
        for(int i=0;i<100;i++){
        	assertNull(map.put(i, i));
        	assertEquals(i, map.get(i));
        }
        IMap<Integer, Integer> map2 = hClient.getMap("default");
        assertTrue(map == map2);
        System.out.println(map.getId());
        assertTrue(map.getId().equals(map2.getId()));
        map.destroy();
        map2 = hClient.getMap("default");
        assertFalse(map == map2);
        for(int i=0;i<100;i++){
        	assertNull(map2.get(i));
        }
    }
    @Test
    public void containsKey(){
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
        IMap<Integer, Integer> map = hClient.getMap("default");
        int counter = 100;
		for(int i=0;i<counter;i++){
        	assertNull(map.put(i, i));
        	assertEquals(i, map.get(i));
        }
        for(int i=0;i<counter;i++){
        	assertTrue(map.containsKey(i));
        }
    }
    @Test
    public void containsValue(){
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
        IMap<Integer, Integer> map = hClient.getMap("default");
        int counter = 100;
		for(int i=0;i<counter;i++){
        	assertNull(map.put(i, i));
        	assertEquals(i, map.get(i));
        }
        for(int i=0;i<counter;i++){
        	assertTrue(map.containsValue(i));
        }
    }
    @Test
    public void isEmpty(){
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
        IMap<Integer, Integer> map = hClient.getMap("default");
        int counter = 100;
		assertTrue(map.isEmpty());
        for(int i=0;i<counter;i++){
        	assertNull(map.put(i, i));
        	assertEquals(i, map.get(i));
        }
        assertFalse(map.isEmpty());
    }
    @Test
    public void putAll(){
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
        IMap<Integer, Integer> map = hClient.getMap("default");
        int counter = 100;
        Map<Integer, Integer> tempMap = new HashMap<Integer, Integer>();
		for(int i=0;i<counter;i++){
			tempMap.put(i,i);
        }
		map.putAll(tempMap);
        for(int i=0;i<counter;i++){
        	assertEquals(i, map.get(i));
        }
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
	
    @Test
    public void testTwoMembersWithIndexes() {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
//        IMap imap = h1.getMap("employees");
        hClient = getHazelcastClient(h1);
        final IMap imap  = hClient.getMap("employees");
        imap.addIndex("name", false);
        imap.addIndex("age", true);
        imap.addIndex("active", false);
        doFunctionalQueryTest(imap);
    }
    
    public void doFunctionalQueryTest(IMap imap) {
        imap.put("1", new Employee("joe", 33, false, 14.56));
        imap.put("2", new Employee("ali", 23, true, 15.00));
        for (int i = 3; i < 103; i++) {
            imap.put(String.valueOf(i), new Employee("name" + i, i % 60, ((i % 2) == 1), Double.valueOf(i)));
        }
        Set<Map.Entry> entries = imap.entrySet();
        assertEquals(102, entries.size());
        int itCount = 0;
        for (Map.Entry entry : entries) {
            Employee c = (Employee) entry.getValue();
            itCount++;
        }
        assertEquals(102, itCount);
        EntryObject e = new PredicateBuilder().getEntryObject();
        Predicate predicate = e.is("active").and(e.get("age").equal(23));
        entries = imap.entrySet(predicate);
        assertEquals(3, entries.size());
        for (Map.Entry entry : entries) {
            Employee c = (Employee) entry.getValue();
            assertEquals(c.getAge(), 23);
            assertTrue(c.isActive());
        }
        imap.remove("2");
        entries = imap.entrySet(predicate);
        assertEquals(2, entries.size());
        for (Map.Entry entry : entries) {
            Employee c = (Employee) entry.getValue();
            assertEquals(c.getAge(), 23);
            assertTrue(c.isActive());
        }
    }
}
