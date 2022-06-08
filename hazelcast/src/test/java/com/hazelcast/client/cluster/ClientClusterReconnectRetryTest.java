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

package com.hazelcast.client.cluster;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.impl.connection.ClientConnectionManager;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class ClientClusterReconnectRetryTest extends HazelcastTestSupport {
    private static final int CLIENT_RECONNECT_SECS = 15;
    // if the test fails because of the slowness, decrease the assertion period below
    private static final int DISCONNECTED_CLIENT_ASSERTION_PERIOD_SECS = CLIENT_RECONNECT_SECS - 5;
    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    @Test
    public void testClientShouldComplyWithConnectionStrategyConfigWhenAttemptingClusterReconnect() {
        ClientConfig clientConfig = new ClientConfig();
        // Set connection retry strategy
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig()
                .setInitialBackoffMillis((int) TimeUnit.SECONDS.toMillis(CLIENT_RECONNECT_SECS))
                .setMaxBackoffMillis((int) TimeUnit.SECONDS.toMillis(CLIENT_RECONNECT_SECS));
        HazelcastInstance member = factory.newHazelcastInstance();
        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        ClientConnectionManager clientConnectionManager = ((HazelcastClientProxy) client).client.getConnectionManager();
        // assert the initial cluster connection
        assertTrueEventually(() -> assertEquals(1, clientConnectionManager.getActiveConnections().size()));

        member.shutdown();
        // wait for the client disconnecting from the cluster
        assertTrueEventually(() -> assertEquals(0, clientConnectionManager.getActiveConnections().size()));
        factory.newHazelcastInstance();
        // assert that the client doesn't reconnect to the cluster in DISCONNECTED_CLIENT_ASSERTION_PERIOD_SECS
        // which is 5 secs less than CLIENT_RECONNECT_SECS
        assertTrueAllTheTime(() -> assertEquals(0, clientConnectionManager.getActiveConnections().size()),
                DISCONNECTED_CLIENT_ASSERTION_PERIOD_SECS);
        // after the CLIENT_RECONNECT_SECS passed, we expect the client to reconnect to the cluster
        assertTrueEventually(() -> assertEquals(1, clientConnectionManager.getActiveConnections().size()));
    }

    @After
    public void cleanup() {
        factory.terminateAll();
    }
}
