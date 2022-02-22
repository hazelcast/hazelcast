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

import com.hazelcast.client.config.ClientConfig;
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

import static com.hazelcast.client.impl.management.ManagementCenterService.MC_CLIENT_MODE_PROP;
import static com.hazelcast.test.Accessors.getClientEngineImpl;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientSelectorManagementCenterClientTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testManagementCenterClient_doesNotGetDisconnected() {
        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();

        final ClientEngineImpl clientEngineImpl = getClientEngineImpl(instance);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(MC_CLIENT_MODE_PROP.getName(), "true");
        hazelcastFactory.newHazelcastClient(clientConfig);

        clientEngineImpl.applySelector(ClientSelectors.none());

        assertTrueAllTheTime(() -> {
            Collection<ClientEndpoint> endpoints = clientEngineImpl.getEndpointManager().getEndpoints();
            assertEquals(1, endpoints.size());
        }, 10);
    }

}
