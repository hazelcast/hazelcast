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

import org.junit.After;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IQueue;
import static com.hazelcast.client.TestUtility.getHazelcastClient;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import java.util.*;

public class HazelcastClientQueueTest {

    private HazelcastClient hClient;

    @After
    public void shutdownAll() throws InterruptedException{
    	Hazelcast.shutdownAll();
    	if(hClient!=null){	hClient.shutdown(); }
    	Thread.sleep(500);
    }

    @Test
    public void testQueueName(){
        HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
    	IQueue<?> queue = hClient.getQueue("ABC");
    	assertEquals("ABC", queue.getName());
    }

    @Test
    public void testQueueOffer() throws InterruptedException {
        HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
    	IQueue<String> queue = hClient.getQueue("ABC");
        assertTrue(queue.offer("a"));
        assertTrue(queue.offer("b", 10, TimeUnit.MILLISECONDS));
        assertEquals("a", queue.poll());
        assertEquals("b", queue.poll());

    }

    @Test
    public void testQueuePoll() throws InterruptedException {
        HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
        hClient = getHazelcastClient(h);
        final CountDownLatch cl = new CountDownLatch(1);
        final IQueue<String> queue = hClient.getQueue("ABC");
        assertTrue(queue.offer("a"));
        assertEquals("a", queue.poll());
        new Thread(new Runnable(){

            public void run() {
                try {
                    Thread.sleep(60);
                    assertEquals("b", queue.poll(100, TimeUnit.MILLISECONDS));
                    cl.countDown();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();
        Thread.sleep(50);
        assertTrue(queue.offer("b"));
        assertTrue(cl.await(50, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testQueueRemove(){
        HazelcastInstance h = Hazelcast.newHazelcastInstance(null);

    	hClient = getHazelcastClient(h);
    	IQueue<String> queue = hClient.getQueue("ABC");
        assertTrue(queue.offer("a"));
        assertEquals("a", queue.remove());
    }

    @Test
    public void testQueuePeek(){
        HazelcastInstance h = Hazelcast.newHazelcastInstance(null);

    	hClient = getHazelcastClient(h);
    	IQueue<String> queue = hClient.getQueue("ABC");
        assertTrue(queue.offer("a"));
        assertEquals("a", queue.peek());
    }

    @Test
    public void element(){
        HazelcastInstance h = Hazelcast.newHazelcastInstance(null);

    	hClient = getHazelcastClient(h);
    	IQueue<String> queue = hClient.getQueue("ABC");
        assertTrue(queue.offer("a"));
        assertEquals("a", queue.element());
    }

    @Test

    public void addAll(){
        HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
    	IQueue<String> queue = hClient.getQueue("ABC");
        List list = new ArrayList();
        list.add("a");
        list.add("b");

        assertTrue(queue.addAll(list));
        assertEquals("a", queue.poll());
        assertEquals("b", queue.poll());

    }

    @Test
    public void clear(){
        HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
    	hClient = getHazelcastClient(h);
    	IQueue<String> queue = hClient.getQueue("ABC");
        List list = new ArrayList();
        list.add("a");
        list.add("b");
        assertTrue(queue.size()==0);
        assertTrue(queue.addAll(list));
        assertTrue(queue.size()==2);
        queue.clear();
        assertTrue(queue.size()==0);
    }

    @Test
    public void containsAll(){
        HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
        hClient = getHazelcastClient(h);
        IQueue<String> queue = hClient.getQueue("ABC");
        List list = new ArrayList();
        list.add("a");
        list.add("b");
        assertTrue(queue.size()==0);
        assertTrue(queue.addAll(list));
        assertTrue(queue.size()==2);
        assertTrue(queue.containsAll(list));
    }

    @Test

    public void equals(){
        HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
        hClient = getHazelcastClient(h);
        IQueue<String> queue = hClient.getQueue("ABC");
        assertEquals(queue, h.getQueue("ABC"));

    }
    @Test
    public void isEmpty(){
        HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
        hClient = getHazelcastClient(h);
        IQueue<String> queue = hClient.getQueue("ABC");
        assertTrue(queue.isEmpty());
        queue.offer("asd");
        assertFalse(queue.isEmpty());
    }

    @Test
    public void iterator(){
        HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
        hClient = getHazelcastClient(h);
        IQueue<String> queue = hClient.getQueue("ABC");
        assertTrue(queue.isEmpty());
        int count = 100;
        Map<Integer, Integer> map = new HashMap<Integer, Integer>();
        for(int i=0;i<count;i++){
            queue.offer(""+i);
            map.put(i,1);
        }
        Iterator<String> it = queue.iterator();
        while(it.hasNext()){
            String o = it.next();
            map.put(Integer.valueOf(o), map.get(Integer.valueOf(o))-1);
        }

        for(int i=0;i<count;i++){
            assertTrue(map.get(i)==0);
        }
    }
    @Test
    public void removeAl(){
        HazelcastInstance h = Hazelcast.newHazelcastInstance(null);
        hClient = getHazelcastClient(h);
        IQueue<String> queue = hClient.getQueue("ABC");
        assertTrue(queue.isEmpty());
        int count = 100;
        Map<Integer, Integer> map = new HashMap<Integer, Integer>();
        for(int i=0;i<count;i++){
            queue.offer(""+i);
            map.put(i,1);
        }
        List list = new ArrayList();
        for(int i=0;i<count/2;i++){
            list.add(""+i);

        }

        queue.removeAll(list);
        assertTrue(queue.size()==count/2);
    }




}
