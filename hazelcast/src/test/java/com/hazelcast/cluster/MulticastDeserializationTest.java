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

package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.config.JavaSerializationFilterConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MulticastConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.internal.serialization.impl.SerializationConstants;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.Packet;
import com.hazelcast.test.annotation.QuickTest;
import example.serialization.TestDeserialized;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertFalse;

/**
 * Tests if deserialization blacklisting works for MutlicastService.
 */
@Category(QuickTest.class)
public class MulticastDeserializationTest {

    private static final int MULTICAST_PORT = 53535;
    private static final String MULTICAST_GROUP = "224.0.0.219";
    // TTL==0 : Restricted to the same host. Won't be output by any interface.
    private static final int MULTICAST_TTL = 0;

    @Before
    @After
    public void killAllHazelcastInstances() throws IOException {
        TestDeserialized.isDeserialized = false;
        HazelcastInstanceFactory.terminateAll();
    }

    /**
     * <pre>
     * Given: Multicast is configured.
     * When: DatagramPacket with a correct Packet comes. The Packet references Java serializer and the serialized object is not a Join message.
     * Then: The object from the Packet is not deserialized.
     * </pre>
     */
    @Test
    public void test() throws Exception {
        Config config = new Config();
        JavaSerializationFilterConfig javaSerializationFilterConfig = new JavaSerializationFilterConfig();
        javaSerializationFilterConfig.setDefaultsDisabled(true);
        javaSerializationFilterConfig.getBlacklist().addClasses(TestDeserialized.class.getName());
        config.getSerializationConfig().setJavaSerializationFilterConfig(javaSerializationFilterConfig);
        NetworkConfig networkConfig = config.getNetworkConfig();
        JoinConfig join = networkConfig.getJoin();
        join.getTcpIpConfig().setEnabled(false);
        MulticastConfig multicastConfig = join.getMulticastConfig();
        multicastConfig.setMulticastPort(MULTICAST_PORT);
        multicastConfig.setMulticastGroup(MULTICAST_GROUP);
        multicastConfig.setMulticastTimeToLive(MULTICAST_TTL);
        multicastConfig.setEnabled(true);

        Hazelcast.newHazelcastInstance(config);
        sendJoinDatagram(new TestDeserialized());
        Thread.sleep(500L);
        assertFalse("Untrusted deserialization is possible", TestDeserialized.isDeserialized);
    }

    private void sendJoinDatagram(Object object) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        try {
            oos.writeObject(object);
        } finally {
            IOUtil.closeResource(oos);
        }
        byte[] data = bos.toByteArray();
        MulticastSocket multicastSocket = null;
        try {
            multicastSocket = new MulticastSocket(MULTICAST_PORT);
            multicastSocket.setTimeToLive(MULTICAST_TTL);
            InetAddress group = InetAddress.getByName(MULTICAST_GROUP);
            multicastSocket.joinGroup(group);
            int msgSize = data.length;

            ByteBuffer bbuf = ByteBuffer.allocate(1 + 4 + msgSize);
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
