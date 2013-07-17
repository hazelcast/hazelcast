/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client;

import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.util.Clock;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class HazelcastClientTopicTest extends HazelcastClientTestBase {

    @Test(expected = NullPointerException.class)
    public void testAddNull() throws InterruptedException {
        HazelcastClient hClient = getHazelcastClient();
        ITopic<?> topic = hClient.getTopic("testAddNull");
        topic.publish(null);
    }

    @Test
    public void testName() {
        HazelcastClient hClient = getHazelcastClient();
        ITopic<?> topic = hClient.getTopic("testName");
        assertEquals("testName", topic.getName());
    }

    @Test
    public void testDestroy() {
        HazelcastClient hClient = getHazelcastClient();
        ITopic<?> topic = hClient.getTopic("testDestroy");
        topic.destroy();
    }

    @Test
    public void addMessageListener() throws InterruptedException {
        HazelcastClient hClient = getHazelcastClient();
        ITopic<String> topic = hClient.getTopic("addMessageListener");
        final CountDownLatch latch = new CountDownLatch(1);
        final String message = "Hazelcast Rocks!";
        topic.addMessageListener(new MessageListener<String>() {
            public void onMessage(Message<String> msg) {
                if (msg.getMessageObject().equals(message)) {
                    latch.countDown();
                }
            }
        });
        topic.publish(message);
        assertTrue(latch.await(10000, TimeUnit.MILLISECONDS));
    }

    @Test
    public void addTwoMessageListener() throws InterruptedException {
        HazelcastClient hClient = getHazelcastClient();
        ITopic<String> topic = hClient.getTopic("addTwoMessageListener");
        final CountDownLatch latch = new CountDownLatch(2);
        final String message = "Hazelcast Rocks!";
        topic.addMessageListener(new MessageListener<String>() {
            public void onMessage(Message<String> msg) {
                if (msg.getMessageObject().equals(message)) {
                    latch.countDown();
                }
            }
        });
        topic.addMessageListener(new MessageListener<String>() {
            public void onMessage(Message<String> msg) {
                if (msg.getMessageObject().equals(message)) {
                    latch.countDown();
                }
            }
        });
        topic.publish(message);
        assertTrue(latch.await(10000, TimeUnit.MILLISECONDS));
    }

    @Test
    public void removeMessageListener() throws InterruptedException {
        HazelcastClient hClient = getHazelcastClient();
        ITopic<String> topic = hClient.getTopic("removeMessageListener");
        final CountDownLatch latch = new CountDownLatch(2);
        final CountDownLatch cp = new CountDownLatch(1);
//        final String message = "Hazelcast Rocks!";
        MessageListener<String> messageListener = new MessageListener<String>() {
            public void onMessage(Message<String> msg) {
//                if (msg.startsWith(message)) {
                System.out.println("Received " + msg + " at " + this);
                latch.countDown();
                cp.countDown();
//                }
            }
        };
        final String message = "message_" + messageListener.hashCode() + "_";
        topic.addMessageListener(messageListener);
        topic.publish(message + "1");
        cp.await();
        topic.removeMessageListener(messageListener);
        topic.publish(message + "2");
        Thread.sleep(50);
        assertEquals(1, latch.getCount());
    }

    @Test
    public void test10TimesRemoveMessageListener() throws InterruptedException {
        ExecutorService ex = Executors.newFixedThreadPool(1);
        final CountDownLatch latch = new CountDownLatch(10);
        ex.execute(new Runnable() {
            public void run() {
                for (int i = 0; i < 10; i++) {
                    try {
                        removeMessageListener();
                        latch.countDown();
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            }
        });
        assertTrue(latch.await(20, TimeUnit.SECONDS));
    }

    @Test
    public void testPerformance() throws InterruptedException {
        HazelcastClient hClient = getHazelcastClient();
        long begin = Clock.currentTimeMillis();
        int count = 10000;
        final ITopic topic = hClient.getTopic("perf");
        ExecutorService ex = Executors.newFixedThreadPool(10);
        final CountDownLatch l = new CountDownLatch(count);
        for (int i = 0; i < count; i++) {
            ex.submit(new Runnable() {
                public void run() {
                    topic.publish("my object");
                    l.countDown();
                }
            });
        }
        assertTrue(l.await(20, TimeUnit.SECONDS));
        long time = Clock.currentTimeMillis() - begin;
        System.out.println("per second: " + count * 1000 / time);
    }

    @Test
    public void add2listenerAndRemoveOne() throws InterruptedException {
        HazelcastClient hClient = getHazelcastClient();
        ITopic<String> topic = hClient.getTopic("removeMessageListener");
        final CountDownLatch latch = new CountDownLatch(4);
        final CountDownLatch cp = new CountDownLatch(2);
        final String message = "Hazelcast Rocks!";
        MessageListener<String> messageListener1 = new MessageListener<String>() {
            public void onMessage(Message<String> msg) {
                if (msg.getMessageObject().startsWith(message)) {
//                    System.out.println("Received "+msg+" at "+ this);
                    latch.countDown();
                    cp.countDown();
                }
            }
        };
        MessageListener<String> messageListener2 = new MessageListener<String>() {
            public void onMessage(Message<String> msg) {
                if (msg.getMessageObject().startsWith(message)) {
//                    System.out.println("Received "+msg+" at "+ this);
                    latch.countDown();
                    cp.countDown();
                }
            }
        };
        topic.addMessageListener(messageListener1);
        topic.addMessageListener(messageListener2);
        topic.publish(message + "1");
        Thread.sleep(50);
        topic.removeMessageListener(messageListener1);
        cp.await();
        topic.publish(message + "2");
        Thread.sleep(100);
        assertEquals(1, latch.getCount());
    }

    @AfterClass
    public static void shutdown() {
    }
}
