package com.hazelcast.nio.tcp;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.PacketUtil;
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

import static com.hazelcast.nio.PacketUtil.toBytePacket;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class TcpIpConnectionManager_TransmitTest extends TcpIpConnection_AbstractTest {
    private List<Packet> packetsB = Collections.synchronizedList(new ArrayList<Packet>());

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
        connManagerA.transmit(null, false, connection);
    }

    @Test
    public void withConnection_whenNullConnection() {
        byte[] packet = toBytePacket(serializationService, "foo", 0, 0);

        boolean result = connManagerA.transmit(packet, false, (Connection) null);

        assertFalse(result);
    }

    @Test
    public void withConnection_whenConnectionWriteFails() {
        Connection connection = mock(Connection.class);
        byte[] packet = toBytePacket(serializationService, "foo", 0, 0);
        when(connection.write(packet, false)).thenReturn(false);

        boolean result = connManagerA.transmit(packet, false, connection);

        assertFalse(result);
    }

    @Test
    public void withConnection_whenConnectionWriteSucceeds() {
        Connection connection = mock(Connection.class);
        byte[] packet = toBytePacket(serializationService, "foo", 0, 0);
        when(connection.write(packet, false)).thenReturn(true);

        boolean result = connManagerA.transmit(packet, false, connection);

        assertTrue(result);
    }

    // =============== tests {@link TcpIpConnectionManager#write(Packet,Address)} ===========

    @Test(expected = NullPointerException.class)
    public void withAddress_whenNullPacket() {
        connManagerB.start();

        connManagerA.transmit(null, false, addressB);
    }

    @Test(expected = NullPointerException.class)
    public void withAddress_whenNullAddress() {
        byte[] packet = toBytePacket(serializationService, "foo", 0, 0);

        connManagerA.transmit(packet, false, (Address) null);
    }

    @Test
    public void withAddress_whenConnectionExists() {
        connManagerB.start();

        final byte[] packet = toBytePacket(serializationService, "foo", 0, 0);
        connect(connManagerA, addressB);

        boolean result = connManagerA.transmit(packet, false, addressB);

        assertTrue(result);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(packetsB.contains(PacketUtil.toPacket(packet)));
            }
        });
    }

    @Test
    public void withAddress_whenConnectionNotExists_thenCreated() {
        connManagerB.start();

        final byte[] packet = toBytePacket(serializationService, "foo", 0, 0);

        boolean result = connManagerA.transmit(packet, false, addressB);

        assertTrue(result);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(packetsB.contains(PacketUtil.toPacket(packet)));
            }
        });
        assertNotNull(connManagerA.getConnection(addressB));
    }

    @Test
    public void withAddress_whenConnectionCantBeEstablished() throws UnknownHostException {
        byte[] packet = toBytePacket(serializationService, "foo", 0, 0);

        boolean result = connManagerA.transmit(packet, false, new Address(addressA.getHost(), 6701));

        // true is being returned because there is no synchronization on the connection being
        // established.
        assertTrue(result);
    }
}
