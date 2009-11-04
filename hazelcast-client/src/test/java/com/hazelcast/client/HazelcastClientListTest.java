package com.hazelcast.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Test;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;

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
    public void addItemListener() throws InterruptedException{
    	HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
        final IList<String> list = hClient.getList("default");
        final CountDownLatch addLatch = new CountDownLatch(1);
        final CountDownLatch removeLatch = new CountDownLatch(1);
        list.addItemListener(new CountDownItemListener<String>(addLatch, removeLatch), true);
        list.add("hello");
        list.remove("hello");
        assertTrue(addLatch.await(10, TimeUnit.MILLISECONDS));
//        assertTrue(removeLatch.await(10, TimeUnit.MILLISECONDS));
    }
    

}
