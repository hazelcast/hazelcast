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

import com.hazelcast.core.*;

import static com.hazelcast.client.TestUtility.*;

public class HazelcastClientSetTest {

    @Test (expected = NullPointerException.class)
    public void testAddNull() throws InterruptedException {
        HazelcastClient hClient = getHazelcastClient();
        ISet<?> set = hClient.getSet("testAddNull");
        set.add(null);

    }

    @Test
    public void getSetName(){
        HazelcastClient hClient = getHazelcastClient();

    	ISet set = hClient.getSet("getSetName");
    	assertEquals("getSetName", set.getName());
    }
    @Test
    public void addRemoveItemListener() throws InterruptedException{
        HazelcastClient hClient = getHazelcastClient();

        final ISet set = hClient.getSet("addRemoveItemListener");
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
        HazelcastClient hClient = getHazelcastClient();

        ISet set = hClient.getSet("destroy");
        for(int i=0;i<100;i++){
        	assertTrue(set.add(i));
        }
        ISet set2 = hClient.getSet("destroy");
        assertTrue(set == set2);
        assertTrue(set.getId().equals(set2.getId()));
        set.destroy();
        set2 = hClient.getSet("destroy");
        assertFalse(set == set2);
//        for(int i=0;i<100;i++){
//        	assertNull(list2.get(i));
//        }
    }
    @Test
    public void add(){
        HazelcastClient hClient = getHazelcastClient();

        ISet set = hClient.getSet("add");
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
        HazelcastClient hClient = getHazelcastClient();

        ISet set = hClient.getSet("contains");
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
        HazelcastClient hClient = getHazelcastClient();

        ISet set= hClient.getSet("addAll");
        List arr = new ArrayList<Integer>();
        int count = 100;
        for(int i=0;i<count;i++){
        	arr.add(i);
        }
        assertTrue(set.addAll(arr));
    }
    
    @Test
    public void containsAll(){
        HazelcastClient hClient = getHazelcastClient();

        ISet set = hClient.getSet("containsAll");
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
        HazelcastClient hClient = getHazelcastClient();

        ISet set = hClient.getSet("size");
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
        HazelcastClient hClient = getHazelcastClient();

        ISet set = hClient.getSet("remove");
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
        HazelcastClient hClient = getHazelcastClient();

        ISet set = hClient.getSet("clear");
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
        HazelcastClient hClient = getHazelcastClient();

        ISet set = hClient.getSet("removeAll");
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
        HazelcastClient hClient = getHazelcastClient();

        ISet set = hClient.getSet("iterate");
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
