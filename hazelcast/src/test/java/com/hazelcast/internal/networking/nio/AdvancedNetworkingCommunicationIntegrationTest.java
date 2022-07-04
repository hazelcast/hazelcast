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

package com.hazelcast.internal.networking.nio;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.ascii.HTTPCommunicator;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.FailureMode;
import net.spy.memcached.MemcachedClient;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.Collections;

import static com.hazelcast.test.MemcacheTestUtil.shutdownQuietly;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class AdvancedNetworkingCommunicationIntegrationTest extends AbstractAdvancedNetworkIntegrationTest {

    @Test
    public void testMemberConnectionToEndpoints() {
        Config config = createCompleteMultiSocketConfig();
        configureTcpIpConfig(config);
        newHazelcastInstance(config);

        startMemberAndTryToJoinToPort(MEMBER_PORT, 2);
        startMemberAndTryToJoinToPort(NOT_OPENED_PORT, 1);
        testMemberJoinFailsOnPort(CLIENT_PORT);
    }

    @Test
    public void testRestConnectionToEndpoints() throws IOException {
        Config config = createCompleteMultiSocketConfig();
        HazelcastInstance hz = newHazelcastInstance(config);

        HTTPCommunicator communicator = new HTTPCommunicator(hz, "/127.0.0.1:" + REST_PORT);
        final String expected = "{\"status\":\"success\","
                + "\"version\":\"" + hz.getCluster().getClusterVersion().toString() + "\"}";
        assertEquals(expected, communicator.getClusterVersion());

        testRestCallFailsOnPort(hz, MEMBER_PORT);
        testRestCallFailsOnPort(hz, CLIENT_PORT);
        testRestCallFailsOnPort(hz, WAN1_PORT);
        testRestCallFailsOnPort(hz, MEMCACHE_PORT);
    }

    @Test
    public void testMemcacheConnectionToEndpoints() throws Exception {
        Config config = createCompleteMultiSocketConfig();
        JoinConfig join = config.getAdvancedNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        HazelcastInstance hz = newHazelcastInstance(config);
        MemcachedClient client = null;
        try {
            client = getMemcachedClient(hz, MEMCACHE_PORT);
            client.get("whatever");
        } finally {
            shutdownQuietly(client);
        }

        testMemcacheCallFailsOnPort(hz, MEMBER_PORT);
        testMemcacheCallFailsOnPort(hz, CLIENT_PORT);
        testMemcacheCallFailsOnPort(hz, WAN1_PORT);
        testMemcacheCallFailsOnPort(hz, REST_PORT);
    }

    private void startMemberAndTryToJoinToPort(int port, int expectedClusterSize) {
        Config config = prepareJoinConfigForSecondMember(port);
        HazelcastInstance newHzInstance = null;
        try {
            newHzInstance = Hazelcast.newHazelcastInstance(config);
            int clusterSize = newHzInstance.getCluster().getMembers().size();
            assertEquals(expectedClusterSize, clusterSize);
        } finally {
            if (newHzInstance != null) {
                newHzInstance.shutdown();
            }
        }
    }

    private void testMemberJoinFailsOnPort(int port) {
        Config config = prepareJoinConfigForSecondMember(port);
        HazelcastInstance newHzInstance = null;
        try {
            newHzInstance = Hazelcast.newHazelcastInstance(config);
            fail("Member join should throw IllegalStateException for port " + port);
        } catch (IllegalStateException ex) {
            // expected
        } finally {
            if (newHzInstance != null) {
                newHzInstance.shutdown();
            }
        }
    }

    private void testRestCallFailsOnPort(HazelcastInstance hz, int port) throws IOException {
        HTTPCommunicator communicator = new HTTPCommunicator(hz, "/127.0.0.1:" + port);
        try {
            communicator.getClusterVersion();
            fail("REST call should throw SocketException for port " + port);
        } catch (SocketException ex) {
            // expected
        }
    }

    private void testMemcacheCallFailsOnPort(HazelcastInstance hz, int port) throws Exception {
        MemcachedClient client = null;
        try {
            client = getMemcachedClient(hz, port);
            try {
                client.get("whatever");
                fail("Memcache call should throw SocketException for port " + port);
            } catch (Exception ex) {
                // expected
            }
        } finally {
            shutdownQuietly(client);
        }
    }

    private MemcachedClient getMemcachedClient(HazelcastInstance instance, int port) throws Exception {
        String hostName = instance.getCluster().getLocalMember().getSocketAddress().getHostName();
        InetSocketAddress address = new InetSocketAddress(hostName, port);
        ConnectionFactory factory = new ConnectionFactoryBuilder()
                .setOpTimeout(3000)
                .setDaemon(true)
                .setFailureMode(FailureMode.Retry)
                .build();
        return new MemcachedClient(factory, Collections.singletonList(address));
    }
}
