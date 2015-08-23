package com.hazelcast.nio.tcp;

import com.hazelcast.nio.Packet;
import com.hazelcast.spi.impl.PacketHandler;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.Repeat;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public abstract class TcpIpConnection_BasicTest extends TcpIpConnection_AbstractTest {

    private List<Packet> packetsB = Collections.synchronizedList(new ArrayList<Packet>());

    @Before
    public void setup() throws Exception {
        super.setup();

        startAllConnectionManagers();

        ioServiceB.packetHandler = new PacketHandler() {
            @Override
            public void handle(Packet packet) throws Exception {
                packetsB.add(packet);
            }
        };
    }

    @Test
    public void write_whenNonUrgent() {
        TcpIpConnection c = connect(connManagerA, addressB);

        Packet packet = new Packet(serializationService.toBytes("foo"));

        boolean result = c.write(packet);

        assertTrue(result);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1, packetsB.size());
            }
        });

        Packet found = packetsB.get(0);
        assertEquals(packet, found);
    }

    @Test
    public void write_whenUrgent() {
        TcpIpConnection c = connect(connManagerA, addressB);

        Packet packet = new Packet(serializationService.toBytes("foo"));
        packet.setHeader(Packet.HEADER_URGENT);

        boolean result = c.write(packet);

        assertTrue(result);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1, packetsB.size());
            }
        });

        Packet found = packetsB.get(0);
        assertEquals(packet, found);
    }

    @Test
    public void lastWriteTime_whenPacketWritten() {
        TcpIpConnection connAB = connect(connManagerA, addressB);

        // we need to sleep some so that the lastWriteTime of the connection gets nice and old.
        // we need this so we can determine the lastWriteTime got updated
        sleepSeconds(5);

        Packet packet = new Packet(serializationService.toBytes("foo"));

        connAB.write(packet);

        // wait for the packet to get written.
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1, packetsB.size());
            }
        });

        long result = connAB.lastWriteTime();

        long current = System.currentTimeMillis();

        // make sure that the lastWrite time is within the given marginOfErrorMs.
        // if we make this marginOfError very small, there is a high chance of spurious failures
        final int marginOfErrorMs = 1000;
        assertTrue(current + marginOfErrorMs > result);
        assertTrue(current - marginOfErrorMs < result);
    }

    @Ignore
    @Repeat
    @Test
    public void lastWriteTime_whenNothingWritten() {
        TcpIpConnection c = connect(connManagerA, addressB);

        long result1 = c.lastWriteTime();
        long result2 = c.lastWriteTime();

        assertEquals(result1, result2);
    }

    // we check the lastReadTime by sending a packet on the local connection, and
    // on the remote side we check the if the lastReadTime is updated
    @Test
    @Ignore
    @Repeat(100)
    public void lastReadTime() {
        TcpIpConnection connAB = connect(connManagerA, addressB);
        TcpIpConnection connBA = connect(connManagerB, addressA);

        // we need to sleep some so that the lastReadTime of the connection gets nice and old.
        // we need this so we can determine the lastReadTime got updated
        sleepSeconds(5);

        Packet packet = new Packet(serializationService.toBytes("foo"));

        connAB.write(packet);

        // wait for the packet to get written.
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1, packetsB.size());
            }
        });

        long result = connBA.lastReadTime();

        long current = System.currentTimeMillis();

        // make sure that the lastWrite time is within the given marginOfErrorMs.
        // if we make this marginOfError very small, there is a high chance of spurious failures
        final int marginOfErrorMs = 1000;
        assertTrue(current + marginOfErrorMs > result);
        assertTrue(current - marginOfErrorMs < result);
    }

    @Test
    public void lastReadTime_whenNothingWritten() {
        TcpIpConnection c = connect(connManagerA, addressB);

        long result1 = c.lastReadTime();
        long result2 = c.lastReadTime();

        assertEquals(result1, result2);
    }

    @Test
    public void write_whenNotAlive() {
        TcpIpConnection c = connect(connManagerA, addressB);
        connManagerA.destroyConnection(c);

        Packet packet = new Packet(serializationService.toBytes("foo"));

        boolean result = c.write(packet);

        assertFalse(result);
    }

    @Test
    public void getInetAddress() {
        TcpIpConnection c = connect(connManagerA, addressB);

        InetAddress result = c.getInetAddress();

        assertEquals(c.getSocketChannelWrapper().socket().getInetAddress(), result);
    }

    @Test
    public void getRemoteSocketAddress() {
        TcpIpConnection c = connect(connManagerA, addressB);

        InetSocketAddress result = c.getRemoteSocketAddress();

        assertEquals(new InetSocketAddress(addressB.getHost(), addressB.getPort()), result);
    }

    @Test
    public void getPort() {
        TcpIpConnection c = connect(connManagerA, addressB);

        int result = c.getPort();

        assertEquals(c.getSocketChannelWrapper().socket().getPort(), result);
    }

    @Test
    public void test_equals() {
        TcpIpConnection connAB = connect(connManagerA, addressB);
        TcpIpConnection connAC = connect(connManagerA, addressC);

        assertEquals(connAB, connAB);
        assertEquals(connAC, connAC);
        assertNotEquals(connAB, null);
        assertNotEquals(connAB, connAC);
        assertNotEquals(connAC, connAB);
        assertNotEquals(connAB, "foo");
    }
}
