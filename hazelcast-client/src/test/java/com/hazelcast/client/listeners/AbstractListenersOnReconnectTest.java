/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.listeners;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertTrue;

public abstract class AbstractListenersOnReconnectTest extends HazelcastTestSupport {

    protected HazelcastInstance client;
    private TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void testListeners() {
        HazelcastInstance h1 = factory.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setRedoOperation(true);
        clientConfig.getNetworkConfig().setConnectionAttemptLimit(Integer.MAX_VALUE);
        client = factory.newHazelcastClient(clientConfig);
        final CountDownLatch connectedLatch = new CountDownLatch(2);
        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                connectedLatch.countDown();
            }
        });

        final AtomicInteger eventCount = new AtomicInteger();
        String registrationId = addListener(eventCount);

        h1.shutdown();

        factory.newHazelcastInstance();
        assertOpenEventually(connectedLatch, 10);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                produceEvent();
                int count = eventCount.get();
                assertTrue("No events generated!", count > 0);
            }
        }, 30);

        assertTrue(removeListener(registrationId));
    }

    protected abstract String addListener(final AtomicInteger eventCount);

    protected abstract void produceEvent();

    protected abstract boolean removeListener(String registrationId);
}
