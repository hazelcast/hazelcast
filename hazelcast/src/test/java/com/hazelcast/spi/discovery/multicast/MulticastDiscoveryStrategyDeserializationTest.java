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

package com.hazelcast.spi.discovery.multicast;

import com.hazelcast.config.Config;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import example.serialization.TestDeserialized;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests if safe-serialization property works in {@link MulticastDiscoveryStrategy}.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MulticastDiscoveryStrategyDeserializationTest {

    private static final String GROUP = "224.0.0.219";
    private static final int PORT = 54328;

    private volatile boolean stop;
    private volatile Thread datadgramsThread;
    private volatile Exception datagramsThreadException;

    @Before
    public void before() {
        HazelcastInstanceFactory.terminateAll();
        TestDeserialized.isDeserialized = false;
        stop = false;
        datagramsThreadException = null;
        datadgramsThread = new Thread(() -> sendDatagrams());
        datadgramsThread.start();
    }

    @After
    public void after() {
        if (datadgramsThread != null) {
            stop = true;
            try {
                datadgramsThread.join();
            } catch (InterruptedException e) {
            }
            datadgramsThread = null;
        }
        assertNull(datagramsThreadException);
        HazelcastInstanceFactory.terminateAll();
        TestDeserialized.isDeserialized = false;

    }

    @Test
    public void testSafeSerialization() throws Exception {
        Config config = createConfig(true);
        Hazelcast.newHazelcastInstance(config);

        TimeUnit.SECONDS.sleep(3);
        assertFalse("Untrusted deserialization is possible", TestDeserialized.isDeserialized);
    }

    @Test
    public void testDefaultPacketFormat() throws Exception {
        Config config = createConfig(null);
        Hazelcast.newHazelcastInstance(config);
        assertTrueEventually(() -> assertTrue("Object was not deserialized", TestDeserialized.isDeserialized), 15);
    }

    private Config createConfig(Boolean safeSerialization) {
        Config config = smallInstanceConfig().setProperty("hazelcast.discovery.enabled", "true");
        DiscoveryStrategyConfig dsc = new DiscoveryStrategyConfig(MulticastDiscoveryStrategy.class.getName())
                .addProperty("group", GROUP).addProperty("port", String.valueOf(PORT));
        if (safeSerialization != null) {
            dsc.addProperty("safe-serialization", safeSerialization.toString());
        }
        JoinConfig joinConfig = config.getNetworkConfig().getJoin();
        joinConfig.getDiscoveryConfig().addDiscoveryStrategyConfig(dsc);
        return config;
    }

    private void sendDatagrams() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        MulticastSocket multicastSocket = null;
        try {
            try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
                oos.writeObject(new TestDeserialized());
            }
            byte[] data = bos.toByteArray();
            multicastSocket = new MulticastSocket(PORT);
            multicastSocket.setTimeToLive(0);
            InetAddress group = InetAddress.getByName(GROUP);
            multicastSocket.joinGroup(group);
            DatagramPacket packet = new DatagramPacket(data, data.length, group, PORT);
            while (!stop) {
                try {
                    multicastSocket.send(packet);
                    TimeUnit.SECONDS.sleep(1);
                } catch (Exception e) {
                }
            }
            multicastSocket.leaveGroup(group);
        } catch (Exception e) {
            datagramsThreadException = e;
            e.printStackTrace();
        } finally {
            if (multicastSocket != null) {
                multicastSocket.close();
            }
        }
    }
}
