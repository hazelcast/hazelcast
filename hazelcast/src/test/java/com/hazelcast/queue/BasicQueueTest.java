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

package com.hazelcast.queue;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.instance.StaticNodeFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * @ali 2/12/13
 */
@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class BasicQueueTest {

    @BeforeClass
    public static void init() {
//        System.setProperty("hazelcast.test.use.network","true");
    }

    @Before
    @After
    public void cleanup() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testOfferPoll() throws Exception {
        Config config = new Config();
        final int count = 100;
        final int insCount = 4;
        final String name = "defQueue";
        final HazelcastInstance[] instances = StaticNodeFactory.newInstances(config, insCount);
        final Random rnd = new Random(System.currentTimeMillis());

        for (int i = 0; i<count; i++){
            int index = rnd.nextInt(insCount);
            IQueue<String> queue = instances[index].getQueue(name);
            queue.offer("item"+i);
        }

        assertEquals(100, instances[0].getQueue(name).size());

        for (int i = 0; i<count; i++){
            int index = rnd.nextInt(insCount);
            IQueue<String> queue = instances[index].getQueue(name);
            String item = queue.poll();
            assertEquals(item, "item" + i);
        }
        assertEquals(0, instances[0].getQueue(name).size());
        assertNull(instances[0].getQueue(name).poll());

    }

    @Test
    public void testOfferPollWithTimeout() throws Exception {
        final String name = "defQueue";
        Config config = new Config();
        final int count = 100;
        config.getQueueConfig(name).setMaxSize(count);
        final int insCount = 4;
        final HazelcastInstance[] instances = StaticNodeFactory.newInstances(config, insCount);
        final IQueue<String> q = instances[0].getQueue(name);
        final Random rnd = new Random(System.currentTimeMillis());

        for (int i = 0; i<count; i++){
            int index = rnd.nextInt(insCount);
            IQueue<String> queue = instances[index].getQueue(name);
            queue.offer("item"+i);
        }

        assertFalse(q.offer("rejected", 1, TimeUnit.SECONDS));
        assertEquals("item0", q.poll());
        assertTrue(q.offer("not rejected", 1, TimeUnit.SECONDS));


        new Thread(){
            public void run() {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                q.poll();
            }
        }.start();
        assertTrue(q.offer("not rejected", 5, TimeUnit.SECONDS));

        assertEquals(count, q.size());

        for (int i = 0; i<count; i++){
            int index = rnd.nextInt(insCount);
            IQueue<String> queue = instances[index].getQueue(name);
            queue.poll();
        }

        assertNull(q.poll(1, TimeUnit.SECONDS));
        assertTrue(q.offer("offered1"));
        assertEquals("offered1",q.poll(1, TimeUnit.SECONDS));


        new Thread(){
            public void run() {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                q.offer("offered2");
            }
        }.start();
        assertEquals("offered2",q.poll(5, TimeUnit.SECONDS));

        assertEquals(0, q.size());

    }
}
