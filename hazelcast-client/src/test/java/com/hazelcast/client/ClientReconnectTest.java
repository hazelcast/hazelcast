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

import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientReconnectTest extends HazelcastTestSupport {

    private HazelcastInstance server;
    private HazelcastInstance client;

    @Before
    public void setup() {
        server = Hazelcast.newHazelcastInstance();
        client = HazelcastClient.newHazelcastClient();
    }

    @After
    public void teardown() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testClientReconnectOnClusterDown() {
        final CountDownLatch connectedLatch = new CountDownLatch(2);
        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                connectedLatch.countDown();
            }
        });

        IMap<String, String> m = client.getMap("default");

        server.shutdown();
        Hazelcast.newHazelcastInstance();

        assertOpenEventually(connectedLatch, 10);
        assertNull(m.put("test", "test"));
        assertEquals("test", m.get("test"));
    }

    @Test
    public void testClientReconnectOnClusterDownWithEntryListeners() {
        final CountDownLatch connectedLatch = new CountDownLatch(2);
        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                connectedLatch.countDown();
            }
        });

        IMap<String, String> m = client.getMap("default");
        final CountDownLatch latch = new CountDownLatch(1);
        EntryAdapter<String, String> listener = new EntryAdapter<String, String>() {
            public void onEntryEvent(EntryEvent<String, String> event) {
                latch.countDown();
            }
        };
        m.addEntryListener(listener, true);

        server.shutdown();
        Hazelcast.newHazelcastInstance();

        assertOpenEventually(connectedLatch, 10);
        m.put("key", "value");
        assertOpenEventually(latch, 10);
    }
}
