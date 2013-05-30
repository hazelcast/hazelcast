/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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
 */

package com.hazelcast.client.queue;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.core.*;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.annotation.SerialTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * @ali 5/19/13
 */
@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(SerialTest.class)
public class ClientQueueTest {

    static final String queueName = "test1";
    static HazelcastInstance hz;
    static HazelcastInstance server;
    static HazelcastInstance second;
    static IQueue q;

    @BeforeClass
    public static void init(){
        Config config = new Config();
        QueueConfig queueConfig = config.getQueueConfig(queueName);
        queueConfig.setMaxSize(6);
        server = Hazelcast.newHazelcastInstance(config);
        hz = HazelcastClient.newHazelcastClient(null);
        q = hz.getQueue(queueName);
    }

    @AfterClass
    public static void destroy() {
        hz.getLifecycleService().shutdown();
        Hazelcast.shutdownAll();
    }

    @Before
    @After
    public void clear() throws IOException {
        q.clear();
    }

    @Test
    public void testListener() throws Exception {

        final CountDownLatch latch = new CountDownLatch(6);

        ItemListener listener = new ItemListener() {

            public void itemAdded(ItemEvent itemEvent) {
                latch.countDown();
            }

            public void itemRemoved(ItemEvent item) {
            }
        };
        q.addItemListener(listener, true);

        new Thread(){
            public void run() {
                for (int i=0; i<5; i++){
                    q.offer("item" + i);
                }
                q.offer("done");
            }
        }.start();
        assertTrue(latch.await(20, TimeUnit.SECONDS));

    }

    @Test
    public void testOfferPoll() throws IOException, InterruptedException {
        for (int i=0; i<10; i++){
            boolean result = q.offer("item");
            if (i<6){
                assertTrue(result);
            }
            else {
                assertFalse(result);
            }
        }
        assertEquals(6, q.size());

        new Thread(){
            public void run() {
                try {
                    Thread.sleep(2*1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                q.poll();
            }
        }.start();

        boolean result = q.offer("item",5, TimeUnit.SECONDS);
        assertTrue(result);


        for (int i=0; i<10; i++){
            Object o = q.poll();
            if (i<6){
                assertNotNull(o);
            }
            else {
                assertNull(o);
            }
        }
        assertEquals(0, q.size());


        new Thread(){
            public void run() {
                try {
                    Thread.sleep(2*1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                q.offer("item1");
            }
        }.start();

        Object o = q.poll(5, TimeUnit.SECONDS);
        assertEquals("item1", o);
    }

    @Test
    public void testRemainingCapacity() throws IOException {
        assertEquals(6, q.remainingCapacity());
        q.offer("item");
        assertEquals(5, q.remainingCapacity());
    }

    @Test
    public void testRemove() throws IOException {
        assertTrue(q.offer("item1"));
        assertTrue(q.offer("item2"));
        assertTrue(q.offer("item3"));

        assertFalse(q.remove("item4"));
        assertEquals(3, q.size());

        assertTrue(q.remove("item2"));

        assertEquals(2, q.size());

        assertEquals("item1", q.poll());
        assertEquals("item3", q.poll());
    }

    @Test
    public void testContains() {
        assertTrue(q.offer("item1"));
        assertTrue(q.offer("item2"));
        assertTrue(q.offer("item3"));
        assertTrue(q.offer("item4"));
        assertTrue(q.offer("item5"));


        assertTrue(q.contains("item3"));
        assertFalse(q.contains("item"));

        List list = new ArrayList(2);
        list.add("item4");
        list.add("item2");

        assertTrue(q.containsAll(list));

        list.add("item");
        assertFalse(q.containsAll(list));

    }

    @Test
    public void testDrain() {
        assertTrue(q.offer("item1"));
        assertTrue(q.offer("item2"));
        assertTrue(q.offer("item3"));
        assertTrue(q.offer("item4"));
        assertTrue(q.offer("item5"));

        List list = new LinkedList();
        int result = q.drainTo(list, 2);
        assertEquals(2, result);
        assertEquals("item1", list.get(0));
        assertEquals("item2", list.get(1));

        list = new LinkedList();
        result = q.drainTo(list);
        assertEquals(3, result);
        assertEquals("item3", list.get(0));
        assertEquals("item4", list.get(1));
        assertEquals("item5", list.get(2));
    }

    @Test
    public void testIterator(){
        assertTrue(q.offer("item1"));
        assertTrue(q.offer("item2"));
        assertTrue(q.offer("item3"));
        assertTrue(q.offer("item4"));
        assertTrue(q.offer("item5"));

        int i=0;
        for (Object o : q) {
            i++;
            assertEquals("item"+i, o);
        }

    }

    @Test
    public void testToArray(){
        assertTrue(q.offer("item1"));
        assertTrue(q.offer("item2"));
        assertTrue(q.offer("item3"));
        assertTrue(q.offer("item4"));
        assertTrue(q.offer("item5"));

        Object[] array =  q.toArray();
        int i=0;
        for (Object o : array) {
            i++;
            assertEquals("item"+i, o);
        }

        Object[] objects = q.toArray(new Object[2]);
        i=0;
        for (Object o : objects) {
            i++;
            assertEquals("item"+i, o);
        }
    }

    @Test
    public void testAddAll() throws IOException {
        Collection coll = new ArrayList(4);
        coll.add("item1");
        coll.add("item2");
        coll.add("item3");
        coll.add("item4");
        assertTrue(q.addAll(coll));
        int size = q.size();
        assertEquals(size, coll.size());

    }

    @Test
    public void testRemoveRetain() throws IOException {
        assertTrue(q.offer("item1"));
        assertTrue(q.offer("item2"));
        assertTrue(q.offer("item3"));
        assertTrue(q.offer("item4"));
        assertTrue(q.offer("item5"));

        List list = new LinkedList();
        list.add("item8");
        list.add("item9");
        assertFalse(q.removeAll(list));
        assertEquals(5, q.size());

        list.add("item3");
        list.add("item4");
        list.add("item1");
        assertTrue(q.removeAll(list));
        assertEquals(2, q.size());

        list.clear();
        list.add("item2");
        list.add("item5");
        assertFalse(q.retainAll(list));
        assertEquals(2, q.size());

        list.clear();
        assertTrue(q.retainAll(list));
        assertEquals(0, q.size());
    }

    @Test
    public void testClear(){
        assertTrue(q.offer("item1"));
        assertTrue(q.offer("item2"));
        assertTrue(q.offer("item3"));
        assertTrue(q.offer("item4"));
        assertTrue(q.offer("item5"));

        q.clear();

        assertEquals(0, q.size());
        assertNull(q.poll());

    }
}
