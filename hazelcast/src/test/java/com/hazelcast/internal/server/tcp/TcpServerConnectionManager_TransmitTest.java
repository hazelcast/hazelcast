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

package com.hazelcast.internal.server.tcp;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.instance.EndpointQualifier.MEMBER;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class TcpServerConnectionManager_TransmitTest
        extends TcpServerConnection_AbstractTest {
    private List<Packet> packetsB = Collections.synchronizedList(new ArrayList<>());

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        tcpServerA.start();

        serverContextB.packetConsumer = packet -> packetsB.add(packet);
    }

    // =============== tests {@link TcpIpConnectionManager#write(Packet,Address)} ===========

    @Test(expected = NullPointerException.class)
    public void withAddress_whenNullPacket() {
        tcpServerB.start();

        tcpServerA.getConnectionManager(MEMBER).transmit(null, addressB);
    }

    @Test(expected = NullPointerException.class)
    public void withAddress_whenNullAddress() {
        Packet packet = new Packet(serializationService.toBytes("foo"));

        tcpServerA.getConnectionManager(MEMBER).transmit(packet, (Address) null);
    }

    @Test
    public void withAddress_whenConnectionExists() {
        tcpServerB.start();

        final Packet packet = new Packet(serializationService.toBytes("foo"));
        connect(tcpServerA, addressB);

        boolean result = tcpServerA.getConnectionManager(MEMBER).transmit(packet, addressB);

        assertTrue(result);
        assertTrueEventually(() -> assertContains(packetsB, packet));
    }

    @Test
    public void withAddress_whenConnectionNotExists_thenCreated() {
        tcpServerB.start();

        final Packet packet = new Packet(serializationService.toBytes("foo"));

        boolean result = tcpServerA.getConnectionManager(MEMBER).transmit(packet, addressB);

        assertTrue(result);
        assertTrueEventually(() -> assertContains(packetsB, packet));
        assertNotNull(tcpServerA.getConnectionManager(MEMBER).get(addressB));
    }

    @Test
    public void withAddress_whenConnectionCantBeEstablished() throws UnknownHostException {
        final Packet packet = new Packet(serializationService.toBytes("foo"));

        boolean result = tcpServerA.getConnectionManager(MEMBER).transmit(packet, new Address(addressA.getHost(), 6701));

        // true is being returned because there is no synchronization on the connection being established
        assertTrue(result);
    }
}
