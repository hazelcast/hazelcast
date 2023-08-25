/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.connection.ClientConnectionManager;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.client.impl.clientside.ClientTestUtil.getHazelcastClientInstanceImpl;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class ClientClusterReconnectionRetryTest extends HazelcastTestSupport {

    private static final int ASSERTION_SECONDS = 30;

    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void teardown() {
        factory.terminateAll();
    }

    @Test
    public void testClientShouldNotTryToConnectCluster_whenThereIsNoConnection() {
        ClientConfig clientConfig = new ClientConfig();

        // Sleep indefinitely after the first connection attempt fails
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig()
                .setInitialBackoffMillis(Integer.MAX_VALUE)
                .setMaxBackoffMillis(Integer.MAX_VALUE);

        HazelcastInstance member = factory.newHazelcastInstance(getMemberConfig());
        HazelcastInstance client = factory.newHazelcastClient(clientConfig);

        factory.terminate(member);

        ClientConnectionManager connectionManager = getHazelcastClientInstanceImpl(client)
                .getConnectionManager();

        // Wait until the client-side connection is closed
        assertTrueEventually(() -> assertTrue(connectionManager.getActiveConnections().isEmpty()));

        // Wait a bit more to make it more likely that the first reconnection
        // attempt is made before we restart the instance
        sleepAtLeastSeconds(ASSERTION_SECONDS);

        // Cleanup the address registry so that the member will re-use the
        // address it had. This is there to make sure that in the periodic
        // task, we would try to connect the restarted instance(as it was in
        // the member list) and fail the test in case of the incorrect behavior
        factory.cleanup();

        factory.newHazelcastInstance(getMemberConfig());

        // Assert that the client doesn't reconnect to the cluster, as the
        // configured connection strategy would sleep indefinitely after the
        // first connection attempt
        assertTrueAllTheTime(
                () -> assertTrue(connectionManager.getActiveConnections().isEmpty()),
                ASSERTION_SECONDS);
    }

    private Config getMemberConfig() {
        Config config = smallInstanceConfig();
        // Jet prints too many logs while the member is idle
        config.getJetConfig().setEnabled(false);
        return config;
    }

}
