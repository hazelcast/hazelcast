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
import com.hazelcast.core.ItemListener;

import static com.hazelcast.client.TestUtility.*;

public class HazelcastClientListTest {
    private HazelcastClient hClient;

    @After
    public void shutdownAll() throws InterruptedException{
    	Hazelcast.shutdownAll();
    	if(hClient!=null){	hClient.shutdown(); }
    	Thread.sleep(500);
    }
    @Test
    public void getListName(){
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
    	IList<?> list = hClient.getList("ABC");
    	assertEquals("ABC", list.getName());
    }
    @Test
    public void addRemoveItemListener() throws InterruptedException{
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
        final IList list = hClient.getList("default");
        final CountDownLatch addLatch = new CountDownLatch(4);
        final CountDownLatch removeLatch = new CountDownLatch(4);
        ItemListener<String> listener  = new CountDownItemListener<String>(addLatch, removeLatch);
        list.addItemListener(listener, true);
        list.add("hello");
        list.add("hello");
        list.remove("hello");
        list.remove("hello");
        list.removeItemListener(listener);
        list.add("hello");
        list.add("hello");
        list.remove("hello");
        list.remove("hello");
        assertEquals(2, addLatch.getCount());
        assertEquals(2, removeLatch.getCount());
    }
    
    @Test
    public void destroy(){
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
        IList list = hClient.getList("default");
        for(int i=0;i<100;i++){
        	assertTrue(list.add(i));
        }
        IList list2 = hClient.getList("default");
        assertTrue(list == list2);
        assertTrue(list.getId().equals(list2.getId()));
        list.destroy();
        list2 = hClient.getList("default");
        assertFalse(list == list2);
//        for(int i=0;i<100;i++){
//        	assertNull(list2.get(i));
//        }
    }
    @Test
    public void add(){
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
        IList list = hClient.getList("default");
        int count = 10000;
        for(int i=0;i<count;i++){
        	assertTrue(list.add(i));
        }
    }
    @Test
    public void contains(){
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
        IList list = hClient.getList("default");
        int count = 10000;
        for(int i=0;i<count;i++){
        	list.add(i);
        }
        
        for(int i=0;i<count;i++){
        	assertTrue(list.contains(i));
        }
        for(int i=count;i<2*count;i++){
        	assertFalse(list.contains(i));
        }
    }
    
    @Test
    public void addAll(){
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
        IList list = hClient.getList("default");
        List arrList = new ArrayList<Integer>();
        int count = 100;
        for(int i=0;i<count;i++){
        	arrList.add(i);
        }
        assertTrue(list.addAll(arrList));
    }
    
    @Test
    public void containsAll(){
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
        IList list = hClient.getList("default");
        List arrList = new ArrayList<Integer>();
        int count = 100;
        for(int i=0;i<count;i++){
        	arrList.add(i);
        }
        assertTrue(list.addAll(arrList));
        assertTrue(list.containsAll(arrList));
        
        arrList.set((int)count/2, count+1);
        assertFalse(list.containsAll(arrList));
    }
    
    @Test
    public void size(){
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
        IList list = hClient.getList("default");
        int count = 100;
        assertTrue(list.isEmpty());
        for(int i=0;i<count;i++){
        	assertTrue(list.add(i));
        }
        assertEquals(count, list.size());
        for(int i=0;i<count/2;i++){
        	assertTrue(list.add(i));
        }
        assertFalse(list.isEmpty());
        assertEquals(count+count/2, list.size());
    }
    
    @Test
    public void remove(){
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
        IList list = hClient.getList("default");
        int count = 100;
        assertTrue(list.isEmpty());
        for(int i=0;i<count;i++){
        	assertTrue(list.add(i));
        }
        assertEquals(count, list.size());
        for(int i=0;i<count;i++){
        	assertTrue(list.remove((Object)i));
        }
        assertTrue(list.isEmpty());

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }


        for(int i=0;i<count;i++){
            assertFalse(list.remove((Object)i));
        }


        for(int i=count;i<2*count;i++){
        	assertFalse(list.remove((Object)i));
        }
    }
    
    @Test
    public void clear(){
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
        IList list = hClient.getList("default");
        int count = 100;
        assertTrue(list.isEmpty());
        for(int i=0;i<count;i++){
        	assertTrue(list.add(i));
        }
        assertEquals(count, list.size());
        list.clear();
        assertTrue(list.isEmpty());
    }
    
    @Test
    public void removeAll(){
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
        IList list = hClient.getList("default");
        List arrList = new ArrayList<Integer>();
        int count = 100;
        for(int i=0;i<count;i++){
        	arrList.add(i);
        }
        assertTrue(list.addAll(arrList));
        assertTrue(list.removeAll(arrList));
        assertFalse(list.removeAll(arrList.subList(0, count/10)));
    }
    
    
    @Test
    public void iterate(){
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
        IList list = hClient.getList("default");
        list.add(1);
        list.add(2);
        list.add(2);
        list.add(3);
        assertEquals(4, list.size());
        Map counter = new HashMap();
        counter.put(1,1);
        counter.put(2,2);
        counter.put(3,1);
        for (Iterator<Integer> iterator = list.iterator(); iterator.hasNext();) {
			Integer integer = (Integer) iterator.next();
			counter.put(integer, (Integer)counter.get(integer)-1);
			iterator.remove();
		}
        assertEquals(0,counter.get(1));
        assertEquals(0,counter.get(2));
        assertEquals(0,counter.get(3));
        
        assertTrue(list.isEmpty());
    }
    
    
}
