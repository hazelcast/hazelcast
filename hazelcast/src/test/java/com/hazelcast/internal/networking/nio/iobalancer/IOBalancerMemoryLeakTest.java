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

package com.hazelcast.internal.networking.nio.iobalancer;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.internal.ascii.HTTPCommunicator;
import com.hazelcast.internal.networking.nio.NioEventLoopGroup;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Protocols;
import com.hazelcast.nio.tcp.TcpIpConnectionManager;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.Socket;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class IOBalancerMemoryLeakTest extends HazelcastTestSupport {

    @Before
    @After
    public void killAllHazelcastInstances() throws IOException {
        HazelcastInstanceFactory.terminateAll();
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
        final IOBalancer ioBalancer = getIoBalancer(connectionManager);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                int inPipelineSize = ioBalancer.getInLoadTracker().getPipelines().size();
                int outPipelineSize = ioBalancer.getOutLoadTracker().getPipelines().size();
                assertEquals(0, inPipelineSize);
                assertEquals(0, outPipelineSize);
            }
        });
    }

    @Test
    public void testMemoryLeak_with_SocketConnections() {
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
                    Socket socket;
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
        final IOBalancer ioBalancer = getIoBalancer(connectionManager);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                LoadTracker inLoadTracker = ioBalancer.getInLoadTracker();
                LoadTracker outLoadTracker = ioBalancer.getOutLoadTracker();
                int inPipelineSize = inLoadTracker.getPipelines().size();
                int outPipelineSize = outLoadTracker.getPipelines().size();
                int inLoadCount = inLoadTracker.getPipelineLoadCount().keySet().size();
                int outLoadCount = outLoadTracker.getPipelineLoadCount().keySet().size();
                int inLastLoadCount = inLoadTracker.getLastLoadCounter().keySet().size();
                int outLastLoadCount = outLoadTracker.getLastLoadCounter().keySet().size();
                assertEquals(0, inPipelineSize);
                assertEquals(0, outPipelineSize);
                assertEquals(0, inLoadCount);
                assertEquals(0, outLoadCount);
                assertEquals(0, inLastLoadCount);
                assertEquals(0, outLastLoadCount);
            }
        });
    }

    private static IOBalancer getIoBalancer(TcpIpConnectionManager connectionManager) {
        NioEventLoopGroup threadingModel = (NioEventLoopGroup) connectionManager.getEventLoopGroup();
        return threadingModel.getIOBalancer();
    }
}
