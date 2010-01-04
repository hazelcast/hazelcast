/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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

import static com.hazelcast.client.TestUtility.getHazelcastClient;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Transaction;

public class HazelcastClientTransactionTest {

    @Test
    public void rollbackTransactionMap() {
    	HazelcastInstance hClient = getHazelcastClient();
        Transaction transaction = hClient.getTransaction();
        transaction.begin();
        Map<String, String> map = hClient.getMap("rollbackTransactionMap");
        map.put("1", "A");
        assertEquals("A", map.get("1"));
        transaction.rollback();
        assertNull(map.get("1"));
    }

    @Test
    public void commitTransactionMap() {
    	HazelcastInstance hClient = getHazelcastClient();
        Transaction transaction = hClient.getTransaction();
        transaction.begin();
        Map<String, String> map = hClient.getMap("commitTransactionMap");
        map.put("1", "A");
        assertEquals("A", map.get("1"));
        transaction.commit();
        assertEquals("A", map.get("1"));
    }

    @Test
    public void testTransactionVisibilityFromDifferentThreads() throws InterruptedException {
        HazelcastInstance hClient = getHazelcastClient();
        final CountDownLatch latch = new CountDownLatch(1);
        final Object o = new Object();
        Transaction transaction = hClient.getTransaction();
        transaction.begin();
        final IMap<String, String> map = hClient.getMap("testTransactionVisibilityFromDifferentThreads");
        map.put("1", "A");
        assertEquals("A", map.get("1"));


        new Thread(new Runnable(){
            public void run() {
                assertNull(map.get("1"));
                if(!map.containsKey("1")){
                    latch.countDown();
                }
                synchronized (o){
                    o.notify();
                }
            }
        }).start();

        synchronized (o){
            o.wait();
        }
        transaction.rollback();
        assertNull(map.get("1"));

        assertTrue(latch.await(1, TimeUnit.MICROSECONDS));
    }

    @Test
    public void rollbackTransactionList() {
        HazelcastInstance hClient = getHazelcastClient();

        Transaction transaction = hClient.getTransaction();
        transaction.begin();
        List<String> list = hClient.getList("rollbackTransactionList");
        list.add("Istanbul");
        transaction.rollback();
        assertTrue(list.isEmpty());
    }

    @Test
    public void commitTransactionList() {
        HazelcastInstance hClient = getHazelcastClient();

        Transaction transaction = hClient.getTransaction();
        transaction.begin();
        List<String> list = hClient.getList("commitTransactionList");
        list.add("Istanbul");
        transaction.commit();
        assertTrue(list.contains("Istanbul"));
    }

    @Test
    public void rollbackTransactionSet() {
        HazelcastInstance hClient = getHazelcastClient();

        Transaction transaction = hClient.getTransaction();
        transaction.begin();
        Set<String> set = hClient.getSet("rollbackTransactionSet");
        set.add("Istanbul");
        transaction.rollback();
        assertTrue(set.isEmpty());
    }

    @Test
    public void commitTransactionSet() {
        HazelcastInstance hClient = getHazelcastClient();

        Transaction transaction = hClient.getTransaction();
        transaction.begin();
        Set<String> set = hClient.getSet("commitTransactionSet");
        set.add("Istanbul");
        transaction.commit();
        assertTrue(set.contains("Istanbul"));
    }

     @Test
    public void rollbackTransactionQueue() {
        HazelcastInstance hClient = getHazelcastClient();

        Transaction transaction = hClient.getTransaction();
        transaction.begin();
        Queue<String> q = hClient.getQueue("rollbackTransactionQueue");
        q.offer("Istanbul");
        transaction.rollback();
        assertTrue(q.isEmpty());
    }

    @Test
    public void commitTransactionQueue() {
        HazelcastInstance hClient = getHazelcastClient();

        Transaction transaction = hClient.getTransaction();
        transaction.begin();
        Queue<String> q = hClient.getQueue("commitTransactionQueue");
        q.offer("Istanbul");
        transaction.commit();
        assertEquals("Istanbul", q.poll());
    }
}
