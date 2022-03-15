/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.bluegreen;

import com.hazelcast.client.impl.ClientEndpoint;
import com.hazelcast.client.impl.ClientEngineImpl;
import com.hazelcast.client.impl.ClientSelectors;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.LinkedList;

import static com.hazelcast.test.Accessors.getClientEngineImpl;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientSelectorRaceTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testConcurrency() throws InterruptedException {
        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();

        final ClientEngineImpl clientEngineImpl = getClientEngineImpl(instance);

        LinkedList<Thread> threads = new LinkedList<Thread>();
        int numberOfClients = 100;
        for (int i = 0; i < numberOfClients; i++) {
            Thread thread = new Thread(hazelcastFactory::newHazelcastClient);
            thread.start();
            threads.add(thread);

        }

        clientEngineImpl.applySelector(ClientSelectors.none());

        for (Thread thread : threads) {
            thread.join();
        }

        assertTrueEventually(() -> {
            Collection<ClientEndpoint> endpoints = clientEngineImpl.getEndpointManager().getEndpoints();
            assertEquals(0, endpoints.size());
        });

    }

}
