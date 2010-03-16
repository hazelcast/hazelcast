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

import com.hazelcast.core.ITopic;
import com.hazelcast.core.MessageListener;
import org.junit.AfterClass;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.client.TestUtility.getHazelcastClient;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HazelcastClientTopicTest {

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
    public void addMessageListener() throws InterruptedException {
        HazelcastClient hClient = getHazelcastClient();
        ITopic<String> topic = hClient.getTopic("addMessageListener");
        final CountDownLatch latch = new CountDownLatch(1);
        final String message = "Hazelcast Rocks!";
        topic.addMessageListener(new MessageListener<String>() {
            public void onMessage(String msg) {
                if (msg.equals(message)) {
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
            public void onMessage(String msg) {
                if (msg.equals(message)) {
                    latch.countDown();
                }
            }
        });
        topic.addMessageListener(new MessageListener<String>() {
            public void onMessage(String msg) {
                if (msg.equals(message)) {
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
        final String message = "Hazelcast Rocks!";
        MessageListener<String> messageListener = new MessageListener<String>() {
            public void onMessage(String msg) {
                if (msg.startsWith(message)) {
//                    System.out.println("Received "+msg+" at "+ this);
                    latch.countDown();
                    cp.countDown();
                }
            }
        };
        topic.addMessageListener(messageListener);
        topic.publish(message + "1");
        cp.await();
        Thread.sleep(50);
        topic.removeMessageListener(messageListener);
        topic.publish(message + "2");
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

    @AfterClass
    public static void shutdown() {
    }
}
