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

import com.hazelcast.core.Cluster;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientReconnectTest extends HazelcastTestSupport {

    @After
    @Before
    public void cleanup() throws Exception {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testClientReconnectOnClusterDown() throws Exception {
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance();
        final HazelcastInstance client = HazelcastClient.newHazelcastClient();
        IMap<String, String> m = client.getMap("default");
        h1.shutdown();
        final HazelcastInstance newInstance = Hazelcast.newHazelcastInstance();
        waitForClientReconnect(client, newInstance);
        assertNull(m.put("test", "test"));
        assertEquals("test", m.get("test"));
    }

    @Test
    public void testClientReconnectOnClusterDownWithEntryListeners() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance();
        final HazelcastInstance client = HazelcastClient.newHazelcastClient();
        final IMap<String, String> m = client.getMap("default");
        final CountDownLatch latch = new CountDownLatch(1);
        final EntryAdapter<String, String> listener = new EntryAdapter<String, String>() {
            public void onEntryEvent(EntryEvent<String, String> event) {
                latch.countDown();
            }
        };
        m.addEntryListener(listener, true);
        h1.shutdown();
        Thread.sleep(1000);
        final HazelcastInstance newInstance = Hazelcast.newHazelcastInstance();
        waitForClientReconnect(client, newInstance);
        m.put("key", "value");
        assertOpenEventually(latch, 10);
    }


    public static void waitForClientReconnect(HazelcastInstance client, HazelcastInstance instance) {
        final Cluster cluster = client.getCluster();
        int sleepMillis = 200;
        for (int i = 0; i < 50; i++) {
            final Set<Member> members = cluster.getMembers();
            final Iterator<Member> iterator = members.iterator();
            if (iterator.hasNext()) {
                final Member member = iterator.next();
                if( member.getUuid().equals(instance.getLocalEndpoint().getUuid())){
                    return;
                }
            }
            sleepMillis(sleepMillis);
        }
        throw new AssertionError("Client cannot reconnect in 10 seconds");
    }


}
