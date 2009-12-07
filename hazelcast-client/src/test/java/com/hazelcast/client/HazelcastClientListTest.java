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

    @Test(expected = NullPointerException.class)
    public void addNull(){
        HazelcastClient hClient = getHazelcastClient();
    	IList<?> list = hClient.getList("addNull");
        list.add(null);
    }


    @Test
    public void getListName(){
        HazelcastClient hClient = getHazelcastClient();
    	IList<?> list = hClient.getList("getListName");
    	assertEquals("getListName", list.getName());
    }
    @Test
    public void addRemoveItemListener() throws InterruptedException{
    	HazelcastClient hClient = getHazelcastClient();
        final IList list = hClient.getList("addRemoveItemListener");
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
        Thread.sleep(10);
        assertEquals(2, addLatch.getCount());
        assertEquals(2, removeLatch.getCount());
    }
    
    @Test
    public void destroy(){
        HazelcastClient hClient = getHazelcastClient();

        IList list = hClient.getList("destroy");
        for(int i=0;i<100;i++){
        	assertTrue(list.add(i));
        }
        IList list2 = hClient.getList("destroy");
        assertTrue(list == list2);
        assertTrue(list.getId().equals(list2.getId()));
        list.destroy();
        list2 = hClient.getList("destroy");
        assertFalse(list == list2);
    }
    @Test
    public void add(){
        HazelcastClient hClient = getHazelcastClient();

        IList list = hClient.getList("add");
        int count = 10000;
        for(int i=0;i<count;i++){
        	assertTrue(list.add(i));
        }
    }
    @Test
    public void contains(){
        HazelcastClient hClient = getHazelcastClient();

        IList list = hClient.getList("contains");
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
        HazelcastClient hClient = getHazelcastClient();

        IList list = hClient.getList("addAll");
        List arrList = new ArrayList<Integer>();
        int count = 100;
        for(int i=0;i<count;i++){
        	arrList.add(i);
        }
        assertTrue(list.addAll(arrList));
    }
    
    @Test
    public void containsAll(){
        HazelcastClient hClient = getHazelcastClient();

        IList list = hClient.getList("containsAll");
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
        HazelcastClient hClient = getHazelcastClient();

        IList list = hClient.getList("size");
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
        HazelcastClient hClient = getHazelcastClient();

        IList list = hClient.getList("remove");
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

        for(int i=0;i<count;i++){
            assertFalse(list.remove((Object)i));
        }


        for(int i=count;i<2*count;i++){
        	assertFalse(list.remove((Object)i));
        }
    }
    
    @Test
    public void clear(){
        HazelcastClient hClient = getHazelcastClient();

        IList list = hClient.getList("clear");
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
        HazelcastClient hClient = getHazelcastClient();

        IList list = hClient.getList("removeAll");
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
        HazelcastClient hClient = getHazelcastClient();

        IList list = hClient.getList("iterate");
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
