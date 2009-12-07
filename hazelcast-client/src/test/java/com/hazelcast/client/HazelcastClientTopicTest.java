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
import org.junit.Ignore;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import com.hazelcast.core.*;
import static com.hazelcast.client.TestUtility.getHazelcastClient;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class HazelcastClientTopicTest {

    @Test (expected = NullPointerException.class)
    public void testAddNull() throws InterruptedException {
        HazelcastClient hClient = getHazelcastClient();
        ITopic<?> topic = hClient.getTopic("testAddNull");
        topic.publish(null);

    }
    @Test
    public void testName(){
        HazelcastClient hClient = getHazelcastClient();
        System.out.println("1");
    	ITopic<?> topic = hClient.getTopic("testName");
    	assertEquals("testName", topic.getName());
    }

    @Test
    public void addMessageListener() throws InterruptedException {
        HazelcastClient hClient = getHazelcastClient();
        System.out.println("2");
        ITopic<String> topic = hClient.getTopic("addMessageListener");
        final CountDownLatch latch = new CountDownLatch(1);
        final String message =  "Hazelcast Rocks!";

        topic.addMessageListener(new MessageListener()
        {
            public void onMessage(Object msg) {
                if(msg.equals(message)){
                    latch.countDown();
                }
                System.out.println(msg);
            }
        });

        topic.publish(message);

        assertTrue(latch.await(10, TimeUnit.MILLISECONDS));
    }

    @Test
    public void addTwoMessageListener() throws InterruptedException {
        HazelcastClient hClient = getHazelcastClient();
    	ITopic<String> topic = hClient.getTopic("addTwoMessageListener");
        final CountDownLatch latch = new CountDownLatch(2);
        final String message =  "Hazelcast Rocks!";


        topic.addMessageListener(new MessageListener()
        {
            public void onMessage(Object msg) {
                if(msg.equals(message)){
                    latch.countDown();
                }
            }
        });

        topic.addMessageListener(new MessageListener()
        {
            public void onMessage(Object msg) {
                if(msg.equals(message)){
                    latch.countDown();
                }
            }
        });

        topic.publish(message);

        assertTrue(latch.await(100, TimeUnit.MILLISECONDS));
    }
   @Test
   public void removeMessageListener() throws InterruptedException {
        HazelcastClient hClient = getHazelcastClient();
    	ITopic<String> topic = hClient.getTopic("removeMessageListener");
        final CountDownLatch latch = new CountDownLatch(2);
        final String message =  "Hazelcast Rocks!";

        MessageListener<String> messageListener = new MessageListener()
        {
            public void onMessage(Object msg) {
                if(msg.equals(message)){
                    latch.countDown();
                }
            }
        };

        topic.addMessageListener(messageListener);
        topic.publish(message);

        topic.removeMessageListener(messageListener);

        topic.publish(message);

        assertFalse(latch.await(100, TimeUnit.MILLISECONDS));
    }
}
