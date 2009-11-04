package com.hazelcast.client;

import static com.hazelcast.client.TestUtility.getHazelcastClient;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Map;

import org.junit.After;
import org.junit.Test;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Transaction;

public class HazelcastClientTransactionTest {
	 private HazelcastClient hClient;
	 

    @After
    public void shutdownAll() throws InterruptedException{
    	Hazelcast.shutdownAll();
    	if(hClient!=null){	hClient.shutdown(); }
    	Thread.sleep(500);
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

}
