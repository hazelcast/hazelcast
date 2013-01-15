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

import com.hazelcast.config.Config;
import com.hazelcast.core.*;
import com.hazelcast.core.LifecycleEvent.LifecycleState;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @mdogan 4/24/12
 */

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class ListenerTest {

    @After
    @Before
    public void cleanup() throws Exception {
        Hazelcast.shutdownAll();
        HazelcastClient.shutdownAll();
    }

    /* github issue #183 */
    @Test
    public void testKeyBasedListeners() throws InterruptedException {
        try {
            Config config = new Config();
            HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
            IMap<String, String> map = instance.getMap("map");
            map.put("key1", "value1");
            map.put("key2", "value2");
            map.put("key3", "value3");

            ClientConfig clientConfig = new ClientConfig();
            HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

            final AtomicInteger count = new AtomicInteger(0);
            IMap<String, String> clientMap = client.getMap("map");

            clientMap.addEntryListener(new EntryListener<String, String>() {
                public void entryAdded(EntryEvent<String, String> entryEvent) {
                    count.incrementAndGet();
                }
                public void entryRemoved(EntryEvent<String, String> entryEvent) {
                }
                public void entryUpdated(EntryEvent<String, String> entryEvent) {
                    count.incrementAndGet();
                }
                public void entryEvicted(EntryEvent<String, String> entryEvent) {
                }
            },"key1" , true);

            clientMap.addEntryListener(new EntryListener<String, String>() {
                public void entryAdded(EntryEvent<String, String> entryEvent) {
                    count.incrementAndGet();
                }
                public void entryRemoved(EntryEvent<String, String> entryEvent) {
                }
                public void entryUpdated(EntryEvent<String, String> entryEvent) {
                    System.out.println("event map");
                    count.incrementAndGet();
                }
                public void entryEvicted(EntryEvent<String, String> entryEvent) {
                }
            },"key2" , true);

            map.put("key1", "new-value1");
            Thread.sleep(100);
            Assert.assertEquals(count.get(), 1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testConfigLifecycleListener() throws InterruptedException {
        ClientConfig config = new ClientConfig();
        final BlockingQueue<LifecycleEvent> q = new LinkedBlockingQueue<LifecycleEvent>();
        config.addListener(new LifecycleListener() {
            public void stateChanged(final LifecycleEvent event) {
                q.offer(event);
                System.out.println(event);
            }
        });
        Hazelcast.newHazelcastInstance(null);
        HazelcastClient client = HazelcastClient.newHazelcastClient(config);

        Assert.assertEquals(new LifecycleEvent(LifecycleState.STARTING), q.poll(3, TimeUnit.SECONDS));
        Assert.assertEquals(new LifecycleEvent(LifecycleState.CLIENT_CONNECTION_OPENING), q.poll(3, TimeUnit.SECONDS));
        Assert.assertEquals(new LifecycleEvent(LifecycleState.CLIENT_CONNECTION_OPENED), q.poll(3, TimeUnit.SECONDS));
        Assert.assertEquals(new LifecycleEvent(LifecycleState.STARTED), q.poll(3, TimeUnit.SECONDS));
        client.shutdown();
//        Assert.assertEquals(new LifecycleEvent(LifecycleState.CLIENT_CONNECTION_LOST), q.poll(3, TimeUnit.SECONDS));
        Assert.assertEquals(new LifecycleEvent(LifecycleState.SHUTTING_DOWN), q.poll(3, TimeUnit.SECONDS));
        Assert.assertEquals(new LifecycleEvent(LifecycleState.SHUTDOWN), q.poll(3, TimeUnit.SECONDS));
    }


}
