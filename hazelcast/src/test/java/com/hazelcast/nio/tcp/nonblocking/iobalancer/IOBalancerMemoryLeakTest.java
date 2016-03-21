/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.tcp.nonblocking.iobalancer;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceManager;
import com.hazelcast.internal.ascii.HTTPCommunicator;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Protocols;
import com.hazelcast.nio.tcp.TcpIpConnectionManager;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.Socket;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class IOBalancerMemoryLeakTest extends HazelcastTestSupport {

    @Before
    @After
    public void killAllHazelcastInstances() throws IOException {
        HazelcastInstanceManager.terminateAll();
    }

    @Test
    public void testMemoryLeak_with_RestConnections() throws IOException {
        Config config = new Config();
        config.getGroupConfig().setName(randomName());
        config.setProperty(GroupProperty.REST_ENABLED.getName(), "true");
        config.setProperty(GroupProperty.IO_BALANCER_INTERVAL_SECONDS.getName(), "1");

        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        TcpIpConnectionManager connectionManager = (TcpIpConnectionManager) getConnectionManager(instance);
        for (int i = 0; i < 100; i++) {
            communicator.getClusterInfo();
        }
        final IOBalancer ioBalancer = connectionManager.getIoBalancer();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                int inHandlerSize = ioBalancer.getInLoadTracker().getHandlers().size();
                int outHandlerSize = ioBalancer.getOutLoadTracker().getHandlers().size();
                Assert.assertEquals(0, inHandlerSize);
                Assert.assertEquals(0, outHandlerSize);
            }
        });
    }

    @Test
    public void testMemoryLeak_with_SocketConnections() throws IOException {
        Config config = new Config();
        config.getGroupConfig().setName(randomName());
        config.setProperty(GroupProperty.IO_BALANCER_INTERVAL_SECONDS.getName(), "1");
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        final Address address = instance.getCluster().getLocalMember().getAddress();
        int threadCount = 10;
        final int connectionCountPerThread = 100;

        Runnable runnable = new Runnable() {
            public void run() {
                for (int i = 0; i < connectionCountPerThread; i++) {
                    Socket socket = null;
                    try {
                        socket = new Socket(address.getHost(), address.getPort());
                        socket.getOutputStream().write(Protocols.CLUSTER.getBytes());
                        sleepMillis(1000);
                        socket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        };

        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            threads[i] = new Thread(runnable);
            threads[i].start();
        }

        assertJoinable(threads);

        TcpIpConnectionManager connectionManager = (TcpIpConnectionManager) getConnectionManager(instance);
        final IOBalancer ioBalancer = connectionManager.getIoBalancer();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                LoadTracker inLoadTracker = ioBalancer.getInLoadTracker();
                LoadTracker outLoadTracker = ioBalancer.getOutLoadTracker();
                int inHandlerSize = inLoadTracker.getHandlers().size();
                int outHandlerSize = outLoadTracker.getHandlers().size();
                int inHandlerEventsCount = inLoadTracker.getHandlerEventsCounter().keySet().size();
                int outHandlerEventsCount = outLoadTracker.getHandlerEventsCounter().keySet().size();
                int inLastEventsCount = inLoadTracker.getLastEventCounter().keySet().size();
                int outLastEventsCount = outLoadTracker.getLastEventCounter().keySet().size();
                Assert.assertEquals(0, inHandlerSize);
                Assert.assertEquals(0, outHandlerSize);
                Assert.assertEquals(0, inHandlerEventsCount);
                Assert.assertEquals(0, outHandlerEventsCount);
                Assert.assertEquals(0, inLastEventsCount);
                Assert.assertEquals(0, outLastEventsCount);
            }
        });
    }
}
