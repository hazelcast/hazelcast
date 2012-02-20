/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.impl.GroupProperties;
import com.hazelcast.nio.MemberSocketInterceptor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class SocketInterceptorTest {

    @After
    @Before
    public void cleanup() throws Exception {
        Hazelcast.shutdownAll();
        HazelcastClient.shutdownAll();
    }

    @Test(timeout = 120000)
    public void testSuccessfulSocketInterceptor() {
        Config config = new Config();
        SocketInterceptorConfig sic = new SocketInterceptorConfig();
        MySocketInterceptor mySocketInterceptor = new MySocketInterceptor(true);
        sic.setImplementation(mySocketInterceptor);
        config.getNetworkConfig().setSocketInterceptorConfig(sic);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h4 = Hazelcast.newHazelcastInstance(config);
        int count = 1000;
        for (int i = 0; i < count; i++) {
            h1.getMap("default").put(i, "value" + i);
            h2.getMap("default").put(i, "value" + i);
            h3.getMap("default").put(i, "value" + i);
            h4.getMap("default").put(i, "value" + i);
        }
        assertEquals(4, h4.getCluster().getMembers().size());
        assertTrue(mySocketInterceptor.getAcceptCallCount() >= 6);
        assertTrue(mySocketInterceptor.getConnectCallCount() >= 6);
        assertEquals(4, mySocketInterceptor.getInitCallCount());
        assertEquals(0, mySocketInterceptor.getAcceptFailureCount());
        assertEquals(0, mySocketInterceptor.getConnectFailureCount());
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setGroupConfig(new GroupConfig("dev", "dev-pass")).addAddress("localhost");
        MySocketInterceptor myClientSocketInterceptor = new MySocketInterceptor(true);
        clientConfig.setSocketInterceptor(myClientSocketInterceptor);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        for (int i = 0; i < count; i++) {
            client.getMap("default").put(i, "value" + i);
        }
        assertTrue(mySocketInterceptor.getAcceptCallCount() >= 7);
        assertTrue(mySocketInterceptor.getConnectCallCount() >= 6);
        assertEquals(1, myClientSocketInterceptor.getConnectCallCount());
        assertEquals(0, myClientSocketInterceptor.getAcceptCallCount());
        assertEquals(0, mySocketInterceptor.getAcceptFailureCount());
        assertEquals(0, mySocketInterceptor.getConnectFailureCount());
        assertEquals(0, myClientSocketInterceptor.getAcceptFailureCount());
        assertEquals(0, myClientSocketInterceptor.getConnectFailureCount());
    }

    @Test(expected = RuntimeException.class, timeout = 120000)
    public void testFailingSocketInterceptor() {
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_MAX_JOIN_SECONDS, "3");
        SocketInterceptorConfig sic = new SocketInterceptorConfig();
        MySocketInterceptor mySocketInterceptor = new MySocketInterceptor(false);
        sic.setImplementation(mySocketInterceptor);
        config.getNetworkConfig().setSocketInterceptorConfig(sic);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
    }

    @Test(expected = RuntimeException.class, timeout = 120000)
    public void testFailingClientSocketInterceptor() {
        Config config = new Config();
        SocketInterceptorConfig sic = new SocketInterceptorConfig();
        MySocketInterceptor mySocketInterceptor = new MySocketInterceptor(true);
        sic.setImplementation(mySocketInterceptor);
        config.getNetworkConfig().setSocketInterceptorConfig(sic);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        int count = 1000;
        for (int i = 0; i < count; i++) {
            h1.getMap("default").put(i, "value" + i);
            h2.getMap("default").put(i, "value" + i);
        }
        assertEquals(2, h2.getCluster().getMembers().size());
        assertTrue(mySocketInterceptor.getAcceptCallCount() >= 1);
        assertTrue(mySocketInterceptor.getConnectCallCount() >= 1);
        assertEquals(2, mySocketInterceptor.getInitCallCount());
        assertEquals(0, mySocketInterceptor.getAcceptFailureCount());
        assertEquals(0, mySocketInterceptor.getConnectFailureCount());
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setGroupConfig(new GroupConfig("dev", "dev-pass")).addAddress("localhost");
        MySocketInterceptor myClientSocketInterceptor = new MySocketInterceptor(false);
        clientConfig.setSocketInterceptor(myClientSocketInterceptor);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        for (int i = 0; i < count; i++) {
            client.getMap("default").put(i, "value" + i);
        }
        assertTrue(mySocketInterceptor.getAcceptCallCount() >= 2);
        assertTrue(mySocketInterceptor.getConnectCallCount() >= 1);
        assertEquals(1, myClientSocketInterceptor.getConnectCallCount());
        assertEquals(0, myClientSocketInterceptor.getAcceptCallCount());
        assertEquals(1, mySocketInterceptor.getAcceptFailureCount());
        assertEquals(0, myClientSocketInterceptor.getAcceptFailureCount());
        assertEquals(1, myClientSocketInterceptor.getConnectFailureCount());
    }

    public static class MySocketInterceptor implements MemberSocketInterceptor {
        final AtomicInteger initCallCount = new AtomicInteger();
        final AtomicInteger acceptCallCount = new AtomicInteger();
        final AtomicInteger connectCallCount = new AtomicInteger();
        final AtomicInteger acceptFailureCount = new AtomicInteger();
        final AtomicInteger connectFailureCount = new AtomicInteger();
        final boolean successful;

        public MySocketInterceptor(boolean successful) {
            this.successful = successful;
        }

        public void init(SocketInterceptorConfig socketInterceptorConfig) {
            initCallCount.incrementAndGet();
        }

        public void onAccept(Socket acceptedSocket) throws IOException {
            acceptCallCount.incrementAndGet();
            try {
                OutputStream out = acceptedSocket.getOutputStream();
                InputStream in = acceptedSocket.getInputStream();
                int loop = new Random().nextInt(2) + 1;
                int secretValue = 1;
                int expected = (int) Math.pow(2, loop);
                for (int i = 0; i < loop; i++) {
                    out.write(secretValue);
                    int read = in.read();
                    if (read != 2 * secretValue) {
                        throw new IOException("Authentication Failed");
                    }
                    secretValue = read;
                }
                if (secretValue != expected) {
                    throw new IOException("Authentication Failed");
                }
                out.write(0);
            } catch (IOException e) {
                acceptFailureCount.incrementAndGet();
                throw e;
            }
        }

        public void onConnect(Socket connectedSocket) throws IOException {
            connectCallCount.incrementAndGet();
            try {
                OutputStream out = connectedSocket.getOutputStream();
                InputStream in = connectedSocket.getInputStream();
                int multiplyBy = (successful) ? 2 : 1;
                while (true) {
                    int read = in.read();
                    if (read == 0) return;
                    out.write(read * multiplyBy);
                }
            } catch (IOException e) {
                connectFailureCount.incrementAndGet();
                throw e;
            }
        }

        public int getInitCallCount() {
            return initCallCount.get();
        }

        public int getAcceptCallCount() {
            return acceptCallCount.get();
        }

        public int getConnectCallCount() {
            return connectCallCount.get();
        }

        public int getAcceptFailureCount() {
            return acceptFailureCount.get();
        }

        public int getConnectFailureCount() {
            return connectFailureCount.get();
        }

        @Override
        public String toString() {
            return "MySocketInterceptor{" +
                    "initCallCount=" + initCallCount +
                    ", acceptCallCount=" + acceptCallCount +
                    ", connectCallCount=" + connectCallCount +
                    ", acceptFailureCount=" + acceptFailureCount +
                    ", connectFailureCount=" + connectFailureCount +
                    '}';
        }
    }
}
