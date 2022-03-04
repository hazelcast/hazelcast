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

package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

import static com.hazelcast.spi.properties.ClusterProperty.CONNECTION_MONITOR_INTERVAL;
import static com.hazelcast.spi.properties.ClusterProperty.MAX_NO_HEARTBEAT_SECONDS;
import static com.hazelcast.test.HazelcastTestSupport.assertClusterSize;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.randomName;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class TcpIpMemberConnectionLossTest {

    private static final int MEMBER_KICKED_DUE_TO_MISSING_HB = 120;

    @Before
    @After
    public void killAllHazelcastInstances() throws IOException {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void test_whenCanNotReconnect() {
        Config config = getConfig();

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        assertClusterSize(2, h1, h2);

        //when a member disappears and the other one can't connect to it
        h1.getLifecycleService().terminate();
        //then it eventually gets dropped from the cluster
        assertTrueEventually(() -> assertClusterSize(1, h2));
    }

    @Test
    public void test_whenConnectionKeepsDying() throws Exception {
        Config config = getConfig();

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        assertClusterSize(2, h1, h2);

        InetSocketAddress address = Accessors.getNodeEngineImpl(h1).getThisAddress().getInetSocketAddress();
        RejectionProxy rejectionProxy = new RejectionProxy(address.getHostName(), address.getPort());
        try {
            //when a member disappears
            h1.getLifecycleService().terminate();
            //and connections to it don't succeed (they can be established, but immediately drop)
            //(for example a TCP proxy might do this)
            rejectionProxy.start();

            //then it eventually gets dropped from the cluster
            assertTrueEventually(() -> assertClusterSize(1, h2),
                    MEMBER_KICKED_DUE_TO_MISSING_HB / 2);
        } finally {
            rejectionProxy.stop();
        }
    }

    @Nonnull
    private Config getConfig() {
        Config config = new Config();
        config.setClusterName(randomName());
        config.setProperty(MAX_NO_HEARTBEAT_SECONDS.getName(), Integer.toString(MEMBER_KICKED_DUE_TO_MISSING_HB));

        NetworkConfig networkConfig = config.getNetworkConfig();
        JoinConfig join = networkConfig.getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true);
        join.getTcpIpConfig().addMember("127.0.0.1");
        return config;
    }

    private static final class RejectionProxy {

        private final String host;
        private final int port;

        private volatile boolean running;
        private Thread thread;

        RejectionProxy(String host, int port) {
            this.host = host;
            this.port = port;
        }

        void start() {
            running = true;
            thread = new Thread(() -> {
                System.out.println("Starting rejection proxy on port " + port);
                try {
                    try (ServerSocket server = new ServerSocket(port)) {
                        while (running) {
                            try (Socket socket = server.accept()) {
                                MILLISECONDS.sleep(Long.parseLong(CONNECTION_MONITOR_INTERVAL.getDefaultValue()));
                            }
                        }
                    }
                } catch (Exception e) {
                    //ignore
                } finally {
                    System.out.println("Rejection proxy terminated");
                }
            });
            thread.setDaemon(true);
            thread.start();
        }

        void stop() {
            running = false;
            while (thread.isAlive()) {
                //break it out of a potential accept call
                try {
                    new Socket(host, port);
                } catch (IOException e) {
                    //ignore
                }
            }
        }

    }
}
