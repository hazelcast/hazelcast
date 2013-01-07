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

package com.hazelcast.topic.test;

import com.hazelcast.config.*;
import com.hazelcast.core.*;
import com.hazelcast.instance.GroupProperties;
import org.junit.*;
import org.junit.runner.RunWith;

import java.io.FileNotFoundException;
import java.util.concurrent.*;
import static org.junit.Assert.*;

/**
 * Run these tests with
 * -Xms512m -Xmx512m
 */
@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class ClusterTest {

    @BeforeClass
    public static void init() throws Exception {
        System.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "1");
        System.setProperty(GroupProperties.PROP_VERSION_CHECK_ENABLED, "false");
        Hazelcast.shutdownAll();
    }

    @After
    public void cleanup() throws Exception {
        Hazelcast.shutdownAll();
    }

    /**
     * Testing if topic can properly listen messages
     * and if topic has any issue after a shutdown.
     */
    @Test
    public void testTopic() throws FileNotFoundException {
        //final Config cfg = new XmlConfigBuilder("/Users/msk/IdeaProjects/sample/src/main/resources/hazelcast.xml").build();
        final Config cfg = new Config();
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(cfg);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(cfg);
        String topicName = "TestMessages";
        ITopic<String> topic1 = h1.getTopic(topicName);
        final CountDownLatch latch1 = new CountDownLatch(1);
        topic1.addMessageListener(new MessageListener<String>() {
            public void onMessage(Message msg) {
                assertEquals("Test1", msg.getMessageObject());
                latch1.countDown();
            }
        });
        ITopic<String> topic2 = h2.getTopic(topicName);
        final CountDownLatch latch2 = new CountDownLatch(2);
        topic2.addMessageListener(new MessageListener<String>() {
            public void onMessage(Message msg) {
                assertEquals("Test1", msg.getMessageObject());
                latch2.countDown();
            }
        });
        topic1.publish("Test1");
        h1.getLifecycleService().shutdown();
        topic2.publish("Test1");
        try {
            assertTrue(latch1.await(5, TimeUnit.SECONDS));
            assertTrue(latch2.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException ignored) {
        }
    }
}
