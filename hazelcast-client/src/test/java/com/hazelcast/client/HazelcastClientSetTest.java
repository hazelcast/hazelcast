package com.hazelcast.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Test;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.ISet;
import com.hazelcast.core.ItemListener;

import static com.hazelcast.client.TestUtility.*;

public class HazelcastClientSetTest {
    private HazelcastClient hClient;

    @After
    public void shutdownAll() throws InterruptedException{
    	Hazelcast.shutdownAll();
    	if(hClient!=null){	hClient.shutdown(); }
    	Thread.sleep(500);
    }
    @Test
    public void getSetName(){
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
    	ISet set = hClient.getSet("ABC");
    	assertEquals("ABC", set.getName());
    }
    @Test
    public void addRemoveItemListener() throws InterruptedException{
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
        final ISet set = hClient.getSet("default");
        final CountDownLatch addLatch = new CountDownLatch(4);
        final CountDownLatch removeLatch = new CountDownLatch(4);
        ItemListener<String> listener  = new CountDownItemListener<String>(addLatch, removeLatch);
        set.addItemListener(listener, true);
        set.add("hello");
        set.add("hello");
        set.remove("hello");
        set.remove("hello");
        set.removeItemListener(listener);
        set.add("hello");
        set.add("hello");
        set.remove("hello");
        set.remove("hello");
        Thread.sleep(100);
        assertEquals(3, addLatch.getCount());
        assertEquals(3, removeLatch.getCount());
    }
    
    @Test
    public void destroy(){
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
        ISet set = hClient.getSet("default");
        for(int i=0;i<100;i++){
        	assertTrue(set.add(i));
        }
        ISet set2 = hClient.getSet("default");
        assertTrue(set == set2);
        assertTrue(set.getId().equals(set2.getId()));
        set.destroy();
        set2 = hClient.getSet("default");
        assertFalse(set == set2);
//        for(int i=0;i<100;i++){
//        	assertNull(list2.get(i));
//        }
    }
    @Test
    public void add(){
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
        ISet set = hClient.getSet("default");
        int count = 10000;
        for(int i=0;i<count;i++){
        	assertTrue(set.add(i));
        }
        for(int i=0;i<count;i++){
        	assertFalse(set.add(i));
        }
        
        assertEquals(count, set.size());
    }
    @Test
    public void contains(){
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
        ISet set = hClient.getSet("default");
        int count = 10000;
        for(int i=0;i<count;i++){
        	set.add(i);
        }
        
        for(int i=0;i<count;i++){
        	assertTrue(set.contains(i));
        }
        for(int i=count;i<2*count;i++){
        	assertFalse(set.contains(i));
        }
    }
    
    @Test
    public void addAll(){
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
        ISet set= hClient.getSet("default");
        List arr = new ArrayList<Integer>();
        int count = 100;
        for(int i=0;i<count;i++){
        	arr.add(i);
        }
        assertTrue(set.addAll(arr));
    }
    
    @Test
    public void containsAll(){
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
        ISet set = hClient.getSet("default");
        List arrList = new ArrayList<Integer>();
        int count = 100;
        for(int i=0;i<count;i++){
        	arrList.add(i);
        }
        assertTrue(set.addAll(arrList));
        assertTrue(set.containsAll(arrList));
        
        arrList.set((int)count/2, count+1);
        assertFalse(set.containsAll(arrList));
    }
    
    @Test
    public void size(){
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
        ISet set = hClient.getSet("default");
        int count = 100;
        assertTrue(set.isEmpty());
        for(int i=0;i<count;i++){
        	assertTrue(set.add(i));
        }
        assertEquals(count, set.size());
        for(int i=0;i<count/2;i++){
        	assertFalse(set.add(i));
        }
        assertFalse(set.isEmpty());
        assertEquals(count, set.size());
    }
    
    @Test
    public void remove(){
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
        ISet set = hClient.getSet("default");
        int count = 100;
        assertTrue(set.isEmpty());
        for(int i=0;i<count;i++){
        	assertTrue(set.add(i));
        }
        assertEquals(count, set.size());
        for(int i=0;i<count;i++){
        	assertTrue(set.remove((Object)i));
        }
        assertTrue(set.isEmpty());
        
        for(int i=count;i<2*count;i++){
        	assertFalse(set.remove((Object)i));
        }

        for(int i=0;i<count;i++){
        	assertFalse(set.remove((Object)i));
        }
        
    }
    
    @Test
    public void clear(){
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
        ISet set = hClient.getSet("default");
        int count = 100;
        assertTrue(set.isEmpty());
        for(int i=0;i<count;i++){
        	assertTrue(set.add(i));
        }
        assertEquals(count, set.size());
        set.clear();
        assertTrue(set.isEmpty());
    }
    
    @Test
    public void removeAll(){
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
        ISet set = hClient.getSet("default");
        List arrList = new ArrayList<Integer>();
        int count = 100;
        for(int i=0;i<count;i++){
        	arrList.add(i);
        }
        assertTrue(set.addAll(arrList));
        assertTrue(set.removeAll(arrList));
        assertFalse(set.removeAll(arrList.subList(0, count/10)));
    }
    
    
    @Test
    public void iterate(){
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
        ISet set = hClient.getSet("default");
        set.add(1);
        set.add(2);
        set.add(2);
        set.add(3);
        assertEquals(3, set.size());
        Map counter = new HashMap();
        counter.put(1,1);
        counter.put(2,1);
        counter.put(3,1);
        for (Iterator<Integer> iterator = set.iterator(); iterator.hasNext();) {
			Integer integer = (Integer) iterator.next();
			counter.put(integer, (Integer)counter.get(integer)-1);
			iterator.remove();
		}
        assertEquals(0,counter.get(1));
        assertEquals(0,counter.get(2));
        assertEquals(0,counter.get(3));
        
        assertTrue(set.isEmpty());
    }
    
    
}
