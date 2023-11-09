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

package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.config.JavaSerializationFilterConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.serialization.impl.SerializationConstants;
import com.hazelcast.internal.tpcengine.util.OS;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.QuickTest;
import example.serialization.TestDeserialized;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests if deserialization blacklisting works for MulticastService.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MulticastDeserializationTest {

    private static final int MULTICAST_PORT = 53535;
    private static final String MULTICAST_GROUP = "224.0.0.219";
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
        Config config = smallInstanceConfig();
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

    private void sendJoinDatagram(Object object, boolean useBigEndian) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        try {
            oos.writeObject(object);
        } finally {
            closeResource(oos);
        }
        byte[] data = bos.toByteArray();
        MulticastSocket multicastSocket = null;
        try {
            multicastSocket = new MulticastSocket(MULTICAST_PORT);
            multicastSocket.setTimeToLive(MULTICAST_TTL);
            if (OS.isMac()) {
                multicastSocket.setInterface(InetAddress.getByName("127.0.0.1"));
            }
            InetAddress group = InetAddress.getByName(MULTICAST_GROUP);
            multicastSocket.joinGroup(group);
            int msgSize = data.length;

            ByteBuffer bbuf = ByteBuffer.allocate(1 + 4 + msgSize);
            bbuf.order(useBigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
            bbuf.put(Packet.VERSION);
            bbuf.putInt(SerializationConstants.JAVA_DEFAULT_TYPE_SERIALIZABLE);
            bbuf.put(data);
            byte[] packetData = bbuf.array();
            DatagramPacket packet = new DatagramPacket(packetData, packetData.length, group, MULTICAST_PORT);
            multicastSocket.send(packet);
            multicastSocket.leaveGroup(group);
        } finally {
            if (multicastSocket != null) {
                multicastSocket.close();
            }
        }
    }
}
