/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.JavaSerializationFilterConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.instance.impl.JavaIPvXProperties;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.serialization.impl.SerializationConstants;
import com.hazelcast.internal.tpcengine.util.OS;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.TestJavaSerializationUtils;
import com.hazelcast.test.annotation.QuickTest;
import example.serialization.TestDeserialized;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.Serializable;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfigWithoutJetAndMetrics;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests if deserialization blacklisting works for MulticastService.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MulticastDeserializationTest {

    private static final int MULTICAST_PORT = 53535;
    private static final String MULTICAST_GROUP = chooseMulticastGroup();
    // TTL = 0 : restricted to the same host, won't be output by any interface
    private static final int MULTICAST_TTL = 0;

    @Rule
    public OverridePropertyRule multicastGroupOverride = OverridePropertyRule
            .clear(ClusterProperty.MULTICAST_GROUP.getName());

    @Before
    public void before() {
        cleanup();
    }

    @AfterClass
    public static void cleanup() {
        HazelcastInstanceFactory.terminateAll();
        TestDeserialized.isDeserialized = false;
    }

    private static String chooseMulticastGroup() {
        // We must use ipv6 address on BSD localhost interface with ipv6 native sockets
        return OS.isMac() && !JavaIPvXProperties.INSTANCE.preferIPv4Stack() ? "ff11::123" : "224.0.0.219";
    }

    /**
     * Given: Multicast is configured.
     * When: DatagramPacket with a correct Packet comes. The Packet references
     * Java serializer and the serialized object is not a Join message.
     * Then: The object from the Packet is not deserialized.
     */
    @Test
    public void test() throws Exception {
        Config config = createConfig(true);
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        boolean useBigEndian = getNode(instance).getSerializationService().getByteOrder().equals(ByteOrder.BIG_ENDIAN);

        sendJoinDatagram(new TestDeserialized(), useBigEndian);
        Thread.sleep(500L);
        assertFalse("Untrusted deserialization is possible", TestDeserialized.isDeserialized);
    }

    @Test
    public void testWithoutFilter() throws Exception {
        Config config = createConfig(false);
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        boolean useBigEndian = getNode(instance).getSerializationService().getByteOrder().equals(ByteOrder.BIG_ENDIAN);

        sendJoinDatagram(new TestDeserialized(), useBigEndian);
        assertTrueEventually(() -> assertTrue("Object was not deserialized", TestDeserialized.isDeserialized));
    }

    private Config createConfig(boolean withFilter) {
        Config config = smallInstanceConfigWithoutJetAndMetrics();
        if (withFilter) {
            JavaSerializationFilterConfig javaSerializationFilterConfig = new JavaSerializationFilterConfig()
                    .setDefaultsDisabled(true);
            javaSerializationFilterConfig.getBlacklist()
                    .addClasses(TestDeserialized.class.getName());
            config.getSerializationConfig()
                    .setJavaSerializationFilterConfig(javaSerializationFilterConfig);
        }
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getTcpIpConfig()
                .setEnabled(false);
        join.getMulticastConfig()
                .setEnabled(true)
                .setMulticastPort(MULTICAST_PORT)
                .setMulticastGroup(MULTICAST_GROUP)
                .setMulticastTimeToLive(MULTICAST_TTL)
                ;
        return config;
    }

    private void sendJoinDatagram(Serializable object, boolean useBigEndian) throws IOException {
        byte[] data = TestJavaSerializationUtils.serialize(object);
        try (MulticastSocket multicastSocket = new MulticastSocket(MULTICAST_PORT)) {
            multicastSocket.setTimeToLive(MULTICAST_TTL);
            NetworkInterface localhost = NetworkInterface.getByInetAddress(InetAddress.getByName("127.0.0.1"));
            multicastSocket.setNetworkInterface(localhost);
            SocketAddress groupAddress = new InetSocketAddress(InetAddress.getByName(MULTICAST_GROUP), MULTICAST_PORT);
            multicastSocket.joinGroup(groupAddress, localhost);

            int msgSize = data.length;

            ByteBuffer bbuf = ByteBuffer.allocate(1 + 4 + msgSize);
            bbuf.order(useBigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
            bbuf.put(Packet.VERSION);
            bbuf.putInt(SerializationConstants.JAVA_DEFAULT_TYPE_SERIALIZABLE);
            bbuf.put(data);
            byte[] packetData = bbuf.array();
            DatagramPacket packet = new DatagramPacket(packetData, packetData.length, groupAddress);
            multicastSocket.send(packet);
            multicastSocket.leaveGroup(groupAddress, localhost);
        }
    }
}
