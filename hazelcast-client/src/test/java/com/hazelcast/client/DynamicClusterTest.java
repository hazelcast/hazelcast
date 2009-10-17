package com.hazelcast.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.hazelcast.client.core.EntryEvent;
import com.hazelcast.client.core.EntryListener;
import com.hazelcast.client.core.IMap;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class DynamicClusterTest {
	
	@Before
	public void before(){
		
	}
	
	@After
	public void after(){
		Hazelcast.shutdownAll();
	}
	
	@Test
	public void continuePutAndGetIfOneOfConnectedClusterMemberFails() throws InterruptedException, IOException{
		HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
		HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
		HazelcastInstance h3 = Hazelcast.newHazelcastInstance(null);
		Map realMap = Hazelcast.getMap("default");
		
		Map<Integer, HazelcastInstance> memberMap = getMapOfClustorMembers(h1,h2,h3);
		HazelcastClient client = getHazelcastClient(h1,h2,h3);
		
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
		Map realMap = Hazelcast.getMap("default");
		
		Map<Integer, HazelcastInstance> memberMap = getMapOfClustorMembers(h1,h2);
		HazelcastClient client = getHazelcastClient(h1, h2);
		
		Map<String, Integer> map = client.getMap("default");
		int counter = 0;
		while(counter<3){
			System.out.println("COUNTER: "+counter);
			map.put("key",counter);
			assertEquals(counter, map.get("key"));
			
			assertEquals(counter, realMap.get("key"));
			memberMap.get(client.getConnectionManager().getConnection().getAddress().getPort()).shutdown();
			counter++;
		}
	}


	
	 @Test
	    public void addListenerWithTwoMemberClusterAndKillOne() throws InterruptedException, IOException {
	    	HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
	    	HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
	    	Map realMap = Hazelcast.getMap("default");
	    	
	    	Map<Integer, HazelcastInstance> memberMap = getMapOfClustorMembers(	h1, h2);

	    	HazelcastClient client = getHazelcastClient(h1, h2);
	        IMap<String, String> map = client.getMap("default");
	        final CountDownLatch entryAddLatch = new CountDownLatch(2);
	        final CountDownLatch entryUpdatedLatch = new CountDownLatch(2);
	        final CountDownLatch entryRemovedLatch = new CountDownLatch(2);
	        map.addEntryListener(new EntryListener() {
	            public void entryAdded(EntryEvent event) {
	            	System.out.println("Added " + event.getValue());
	                assertEquals("hello", event.getKey());
	                entryAddLatch.countDown();
	            }

	            public void entryRemoved(EntryEvent event) {
	            	System.out.println("removed " + event.getValue());
	                entryRemovedLatch.countDown();
	                assertEquals("hello", event.getKey());
	                assertEquals("new world", event.getValue());
	            }

	            public void entryUpdated(EntryEvent event) {
	            	System.out.println("Updated " + event.getValue());
	            	assertEquals("new world", event.getValue());
	                assertEquals("hello", event.getKey());
	                entryUpdatedLatch.countDown();
	            }

	            public void entryEvicted(EntryEvent event) {
	                entryRemoved(event);
	            }
	        }, true);
	        map.put("hello", "world");
	        map.put("hello", "new world");
	        realMap.remove("hello");
	        memberMap.remove(client.getConnectionManager().getConnection().getAddress().getPort()).shutdown();
	        map.put("hello", "world");
	        map.put("hello", "new world");
	        realMap.remove("hello");
	        assertTrue(entryAddLatch.await(10, TimeUnit.MILLISECONDS));
	        assertTrue(entryUpdatedLatch.await(10, TimeUnit.MILLISECONDS));
	        assertTrue(entryRemovedLatch.await(10, TimeUnit.MILLISECONDS));
	    }

	 
	 
	 
		private Map<Integer, HazelcastInstance> getMapOfClustorMembers(HazelcastInstance ...h) {
			
			Map<Integer, HazelcastInstance> memberMap = new HashMap<Integer, HazelcastInstance>();
	    	for (HazelcastInstance hazelcastInstance : h) {
	    		memberMap.put(hazelcastInstance.getCluster().getLocalMember().getPort(), hazelcastInstance);
			}
			return memberMap;
		}

		private HazelcastClient getHazelcastClient(HazelcastInstance ... h) {
			InetSocketAddress[] addresses = new InetSocketAddress[h.length];
			for (int i = 0; i < h.length; i++) {
				addresses[i] = new InetSocketAddress(h[i].getCluster().getLocalMember().getInetAddress(),h[i].getCluster().getLocalMember().getPort());
			}
			HazelcastClient client = HazelcastClient.getHazelcastClient(addresses);
			return client;
		}
}
