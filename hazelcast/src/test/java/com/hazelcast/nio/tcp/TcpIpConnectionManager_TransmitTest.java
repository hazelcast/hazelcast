/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.tcp;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.impl.PacketHandler;
import com.hazelcast.test.AssertTask;
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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class TcpIpConnectionManager_TransmitTest extends TcpIpConnection_AbstractTest {
    private List<Packet> packetsB = Collections.synchronizedList(new ArrayList<Packet>());

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        connManagerA.start();

        ioServiceB.packetHandler = new PacketHandler() {
            @Override
            public void handle(Packet packet) throws Exception {
                packetsB.add(packet);
            }
        };
    }

    // =============== tests {@link TcpIpConnectionManager#write(Packet,Connection)} ===========

    @Test(expected = NullPointerException.class)
    public void withConnection_whenNullPacket() {
        connManagerB.start();

        TcpIpConnection connection = connect(connManagerA, addressB);
        connManagerA.transmit(null, connection);
    }

    @Test
    public void withConnection_whenNullConnection() {
        Packet packet = new Packet(serializationService.toBytes("foo"));

        boolean result = connManagerA.transmit(packet, (Connection) null);

        assertFalse(result);
    }

    @Test
    public void withConnection_whenConnectionWriteFails() {
        Connection connection = mock(Connection.class);
        Packet packet = new Packet(serializationService.toBytes("foo"));
        when(connection.write(packet)).thenReturn(false);

        boolean result = connManagerA.transmit(packet, connection);

        assertFalse(result);
    }

    @Test
    public void withConnection_whenConnectionWriteSucceeds() {
        Connection connection = mock(Connection.class);
        Packet packet = new Packet(serializationService.toBytes("foo"));
        when(connection.write(packet)).thenReturn(true);

        boolean result = connManagerA.transmit(packet, connection);

        assertTrue(result);
    }

    // =============== tests {@link TcpIpConnectionManager#write(Packet,Address)} ===========

    @Test(expected = NullPointerException.class)
    public void withAddress_whenNullPacket() {
        connManagerB.start();

        connManagerA.transmit(null, addressB);
    }

    @Test(expected = NullPointerException.class)
    public void withAddress_whenNullAddress() {
        Packet packet = new Packet(serializationService.toBytes("foo"));

        connManagerA.transmit(packet, (Address) null);
    }

    @Test
    public void withAddress_whenConnectionExists() {
        connManagerB.start();

        final Packet packet = new Packet(serializationService.toBytes("foo"));
        connect(connManagerA, addressB);

        boolean result = connManagerA.transmit(packet, addressB);

        assertTrue(result);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertContains(packetsB, packet);
            }
        });
    }

    @Test
    public void withAddress_whenConnectionNotExists_thenCreated() {
        connManagerB.start();

        final Packet packet = new Packet(serializationService.toBytes("foo"));

        boolean result = connManagerA.transmit(packet, addressB);

        assertTrue(result);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertContains(packetsB, packet);
            }
        });
        assertNotNull(connManagerA.getConnection(addressB));
    }

    @Test
    public void withAddress_whenConnectionCantBeEstablished() throws UnknownHostException {
        final Packet packet = new Packet(serializationService.toBytes("foo"));

        boolean result = connManagerA.transmit(packet, new Address(addressA.getHost(), 6701));

        // true is being returned because there is no synchronization on the connection being established
        assertTrue(result);
    }
}
