/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.spi.properties.ClientProperty;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.IMap;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipAdapter;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.security.UsernamePasswordCredentials;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientReconnectTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testClientReconnectOnClusterDown() throws Exception {
        final HazelcastInstance h1 = hazelcastFactory.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setConnectionAttemptLimit(Integer.MAX_VALUE);
        final HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        final CountDownLatch connectedLatch = new CountDownLatch(2);
        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                connectedLatch.countDown();
            }
        });
        IMap<String, String> m = client.getMap("default");
        h1.shutdown();
        hazelcastFactory.newHazelcastInstance();
        assertOpenEventually(connectedLatch);
        assertNull(m.put("test", "test"));
        assertEquals("test", m.get("test"));
    }

    @Test
    public void testReconnectToNewInstanceAtSameAddress() throws InterruptedException {
        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();
        Address localAddress = instance.getCluster().getLocalMember().getAddress();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setConnectionAttemptLimit(Integer.MAX_VALUE);
        final HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        final CountDownLatch memberRemovedLatch = new CountDownLatch(1);
        client.getCluster().addMembershipListener(new MembershipAdapter() {
            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                memberRemovedLatch.countDown();
            }
        });

        instance.shutdown();
        final HazelcastInstance instance2 = hazelcastFactory.newHazelcastInstance(localAddress);

        assertOpenEventually(memberRemovedLatch);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertClusterSize(1, client);
                Iterator<Member> iterator = client.getCluster().getMembers().iterator();
                Member member = iterator.next();
                assertEquals(instance2.getCluster().getLocalMember(), member);
            }
        });
    }

    @Test
    public void testClientShutdownIfReconnectionNotPossible() {
        HazelcastInstance server = hazelcastFactory.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setConnectionAttemptLimit(1);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        final CountDownLatch shutdownLatch = new CountDownLatch(1);
        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (event.getState() == LifecycleEvent.LifecycleState.SHUTDOWN) {
                    shutdownLatch.countDown();
                }
            }
        });
        server.shutdown();

        assertOpenEventually(shutdownLatch);
    }

    @Test(expected = HazelcastClientNotActiveException.class)
    public void testRequestShouldFailOnShutdown() {
        final HazelcastInstance server = hazelcastFactory.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setConnectionAttemptLimit(1);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        IMap<Object, Object> test = client.getMap("test");
        test.put("key", "value");
        server.shutdown();
        test.get("key");
    }

    @Test(expected = HazelcastClientNotActiveException.class)
    public void testExceptionAfterClientShutdown() {
        hazelcastFactory.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        IMap<Object, Object> test = client.getMap("test");
        test.put("key", "value");
        client.shutdown();
        //to force weak references to be cleaned and get not active exception from serialization service
        System.gc();
        test.get("key");
    }

    @Test(expected = HazelcastClientNotActiveException.class)
    public void testExceptionAfterClientShutdown_fromClientConnectionManager() {
        hazelcastFactory.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        IMap<Object, Object> test = client.getMap("test");
        test.put("key", "value");
        client.shutdown();
        test.size();
    }

    @Test
    public void testShutdownClient_whenThereIsNoCluster() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().setAsyncStart(true);
        clientConfig.getNetworkConfig()
                .setConnectionAttemptLimit(Integer.MAX_VALUE);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        client.shutdown();
    }

    public abstract static class CustomCredentials extends UsernamePasswordCredentials {

        CustomCredentials() {
        }

        CustomCredentials(String username, String password) {
            super(username, password);
        }

        @Override
        public int getFactoryId() {
            return 1;
        }

        @Override
        public int getClassId() {
            return 1;
        }
    }

    public static class CustomCredentials_takesLong extends CustomCredentials {

        private AtomicInteger count;
        private long sleepMillis;

        CustomCredentials_takesLong(AtomicInteger count, long sleepMillis) {
            this.count = count;
            this.sleepMillis = sleepMillis;
        }

        CustomCredentials_takesLong(String username, String password) {
            super(username, password);
        }

        @Override
        protected void readPortableInternal(PortableReader reader) throws IOException {
            if (count.incrementAndGet() == 1) {
                try {
                    Thread.sleep(sleepMillis);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            super.readPortableInternal(reader);
        }
    }

    @Test
    public void testClientConnected_withFirstAuthenticationTakingLong() throws InterruptedException {
        final AtomicInteger customCredentialsRunCount = new AtomicInteger();
        final long customCredentialsSleepMillis = 30000;
        SerializationConfig serializationConfig = new SerializationConfig();
        serializationConfig.addPortableFactory(1, new PortableFactory() {
            @Override
            public Portable create(int classId) {
                return new CustomCredentials_takesLong(customCredentialsRunCount, customCredentialsSleepMillis);
            }
        });

        Config config = new Config();
        config.setSerializationConfig(serializationConfig);
        hazelcastFactory.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setCredentials(new CustomCredentials_takesLong("dev", "dev-pass"));
        clientConfig.setSerializationConfig(serializationConfig);
        hazelcastFactory.newHazelcastClient(clientConfig);
    }


    @Test
    @Category(SlowTest.class)
    public void testEndpointCleanup_withFirstAuthenticationTakingLong() throws InterruptedException {
        final AtomicInteger customCredentialsRunCount = new AtomicInteger();
        ClientConfig clientConfig = new ClientConfig();
        int heartbeatTimeoutMillis = 5000;
        int authenticationTimeout = heartbeatTimeoutMillis;
        clientConfig.setProperty(ClientProperty.HEARTBEAT_TIMEOUT.getName(), String.valueOf(heartbeatTimeoutMillis));
        clientConfig.setProperty(ClientProperty.HEARTBEAT_INTERVAL.getName(), "1000");
        //Credentials will take longer than connection timeout so that client will close the connection
        final long customCredentialsSleepMillis = authenticationTimeout * 2;

        SerializationConfig serializationConfig = new SerializationConfig();
        serializationConfig.addPortableFactory(1, new PortableFactory() {
            @Override
            public Portable create(int classId) {
                return new CustomCredentials_takesLong(customCredentialsRunCount, customCredentialsSleepMillis);
            }
        });

        Config config = new Config();
        config.setSerializationConfig(serializationConfig);
        final HazelcastInstance instance = hazelcastFactory.newHazelcastInstance(config);
        //make sure credentials that takes long is used
        clientConfig.setCredentials(new CustomCredentials_takesLong("dev", "dev-pass"));
        clientConfig.setSerializationConfig(serializationConfig);
        //make sure that client is not connected back after first attempt
        clientConfig.getNetworkConfig().setConnectionAttemptLimit(1);
        //since client will not be connected, don't block the current test thread
        clientConfig.getConnectionStrategyConfig().setAsyncStart(true);
        hazelcastFactory.newHazelcastClient(clientConfig);
        //Wait for custom credentials to wake up and register endpoint for timed out connection
        Thread.sleep(customCredentialsSleepMillis + 3000);
        // registered endpoint should be removed eventually
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                int size = instance.getClientService().getConnectedClients().size();
                assertEquals(0, size);
            }
        });
    }

    public static class CustomCredentials_retried extends CustomCredentials {

        private static AtomicInteger count = new AtomicInteger();

        public CustomCredentials_retried() {
        }

        CustomCredentials_retried(String username, String password) {
            super(username, password);
        }

        @Override
        public String getPassword() {
            if (count.incrementAndGet() == 1) {
                throw new HazelcastInstanceNotActiveException();
            }
            return super.getPassword();
        }
    }

    @Test
    public void testClientConnected_withFirstAuthenticationRetried() throws InterruptedException {
        SerializationConfig serializationConfig = new SerializationConfig();
        serializationConfig.addPortableFactory(1, new PortableFactory() {
            @Override
            public Portable create(int classId) {
                return new CustomCredentials_retried();
            }
        });

        Config config = new Config();
        config.setSerializationConfig(serializationConfig);
        hazelcastFactory.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        //first authentication should be able to retried by invocation system. No need to do a second invocation.
        //By setting attempt limit to one, we are expecting client to connect in first attempt.
        clientConfig.getNetworkConfig().setConnectionTimeout(30000);
        clientConfig.getNetworkConfig().setConnectionAttemptLimit(1);
        clientConfig.setCredentials(new CustomCredentials_retried("dev", "dev-pass"));
        clientConfig.setSerializationConfig(serializationConfig);
        hazelcastFactory.newHazelcastClient(clientConfig);
    }
}
