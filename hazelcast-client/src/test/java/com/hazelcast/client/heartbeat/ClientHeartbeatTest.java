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

package com.hazelcast.client.heartbeat;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.spi.impl.ConnectionHeartbeatListener;
import com.hazelcast.client.spi.properties.ClientProperty;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientHeartbeatTest extends ClientTestSupport {

    private static final int HEARTBEAT_TIMEOUT_MILLIS = 3000;

    private TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testHeartbeatStoppedEvent() throws InterruptedException {
        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(getClientConfig());

        HazelcastClientInstanceImpl clientImpl = getHazelcastClientInstanceImpl(client);
        ClientConnectionManager connectionManager = clientImpl.getConnectionManager();
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        connectionManager.addConnectionHeartbeatListener(new ConnectionHeartbeatListener() {
            @Override
            public void heartbeatResumed(Connection connection) {
            }

            @Override
            public void heartbeatStopped(Connection connection) {
                countDownLatch.countDown();
            }
        });

        blockMessagesFromInstance(instance, client);
        assertOpenEventually(countDownLatch);
    }

    @Test
    public void testHeartbeatResumedEvent() throws InterruptedException {
        hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(getClientConfig());
        final HazelcastInstance instance2 = hazelcastFactory.newHazelcastInstance();

        // make sure client is connected to instance2
        String keyOwnedByInstance2 = generateKeyOwnedBy(instance2);
        IMap<String, String> map = client.getMap(randomString());
        map.put(keyOwnedByInstance2, randomString());

        HazelcastClientInstanceImpl clientImpl = getHazelcastClientInstanceImpl(client);
        final ClientConnectionManager connectionManager = clientImpl.getConnectionManager();
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        connectionManager.addConnectionHeartbeatListener(new ConnectionHeartbeatListener() {
            @Override
            public void heartbeatResumed(Connection connection) {
                assertEquals(instance2.getCluster().getLocalMember().getAddress(), connection.getEndPoint());
                countDownLatch.countDown();
            }

            @Override
            public void heartbeatStopped(Connection connection) {
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertNotNull(connectionManager.getConnection(instance2.getCluster().getLocalMember().getAddress()));
            }
        });

        blockMessagesFromInstance(instance2, client);
        sleepMillis(HEARTBEAT_TIMEOUT_MILLIS * 2);
        unblockMessagesFromInstance(instance2, client);

        assertOpenEventually(countDownLatch);
    }

    @Test
    public void testInvocation_whenHeartbeatStopped() throws InterruptedException {
        hazelcastFactory.newHazelcastInstance();
        final HazelcastInstance client = hazelcastFactory.newHazelcastClient(getClientConfig());
        final HazelcastInstance instance2 = hazelcastFactory.newHazelcastInstance();

        // Make sure that the partitions are updated as expected with the new member
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                Member instance2Member = instance2.getCluster().getLocalMember();
                Set<Partition> partitions = client.getPartitionService().getPartitions();
                boolean found = false;
                for (Partition p : partitions) {
                    if (p.getOwner().equals(instance2Member)) {
                        found = true;
                        break;
                    }
                }
                assertTrue(found);
            }
        });

        // make sure client is connected to instance2
        String keyOwnedByInstance2 = generateKeyOwnedBy(instance2);
        IMap<String, String> map = client.getMap(randomString());
        map.put(keyOwnedByInstance2, randomString());
        blockMessagesFromInstance(instance2, client);

        expectedException.expect(TargetDisconnectedException.class);
        expectedException.expectMessage(containsString("heartbeat problems"));
        map.put(keyOwnedByInstance2, randomString());
    }

    @Test
    public void testAsyncInvocation_whenHeartbeatStopped() throws Throwable {
        hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(getClientConfig());
        HazelcastInstance instance2 = hazelcastFactory.newHazelcastInstance();

        // make sure client is connected to instance2
        IMap<String, String> map = client.getMap(randomString());
        String keyOwnedByInstance2 = generateKeyOwnedBy(instance2);
        map.put(keyOwnedByInstance2, randomString());

        blockMessagesFromInstance(instance2, client);

        expectedException.expect(TargetDisconnectedException.class);
        expectedException.expectMessage(containsString("heartbeat problems"));
        try {
            map.putAsync(keyOwnedByInstance2, randomString()).get();
        } catch (ExecutionException e) {
            //unwrap exception
            throw e.getCause();
        }
    }

    @Test
    public void testInvocation_whenHeartbeatResumed() throws InterruptedException {
        hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(getClientConfig());
        HazelcastInstance instance2 = hazelcastFactory.newHazelcastInstance();

        // make sure client is connected to instance2
        String keyOwnedByInstance2 = generateKeyOwnedBy(instance2);
        IMap<String, String> map = client.getMap(randomString());
        map.put(keyOwnedByInstance2, randomString());

        blockMessagesFromInstance(instance2, client);
        sleepMillis(HEARTBEAT_TIMEOUT_MILLIS * 2);
        unblockMessagesFromInstance(instance2, client);

        map.put(keyOwnedByInstance2, randomString());
    }

    private static ClientConfig getClientConfig() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(ClientProperty.HEARTBEAT_TIMEOUT.getName(), String.valueOf(HEARTBEAT_TIMEOUT_MILLIS));
        clientConfig.setProperty(ClientProperty.HEARTBEAT_INTERVAL.getName(), "500");
        return clientConfig;
    }
}
