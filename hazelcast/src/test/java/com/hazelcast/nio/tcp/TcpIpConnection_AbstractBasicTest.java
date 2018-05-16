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

package com.hazelcast.nio.tcp;

import com.hazelcast.nio.Packet;
import com.hazelcast.test.AssertTask;
import com.hazelcast.util.function.Consumer;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.lang.System.currentTimeMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public abstract class TcpIpConnection_AbstractBasicTest extends TcpIpConnection_AbstractTest {

    // sleep time for lastWrite and lastRead tests
    private static final int LAST_READ_WRITE_SLEEP_SECONDS = 5;

    // if we make this MARGIN_OF_ERROR_MS very small, there is a high chance of spurious failures
    private static final int MARGIN_OF_ERROR_MS = 3000;

    private List<Packet> packetsB;

    @Before
    public void setup() throws Exception {
        super.setup();
        packetsB = Collections.synchronizedList(new ArrayList<Packet>());
        startAllConnectionManagers();
        ioServiceB.packetConsumer = new Consumer<Packet>() {
            @Override
            public void accept(Packet packet) {
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
            public void run() {
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
        packet.raiseFlags(Packet.FLAG_URGENT);

        boolean result = c.write(packet);

        assertTrue(result);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(1, packetsB.size());
            }
        });

        Packet found = packetsB.get(0);
        assertEquals(packet, found);
    }

    @Test
    public void lastWriteTimeMillis_whenPacketWritten() {
        TcpIpConnection connAB = connect(connManagerA, addressB);

        // we need to sleep some so that the lastWriteTime of the connection gets nice and old.
        // we need this so we can determine the lastWriteTime got updated
        sleepSeconds(LAST_READ_WRITE_SLEEP_SECONDS);

        Packet packet = new Packet(serializationService.toBytes("foo"));

        connAB.write(packet);

        // wait for the packet to get written
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(1, packetsB.size());
            }
        });

        long lastWriteTimeMs = connAB.lastWriteTimeMillis();
        long nowMs = currentTimeMillis();

        // make sure that the lastWrite time is within the given MARGIN_OF_ERROR_MS

        // last write time should be equal or smaller than now
        assertTrue("nowMs = " + nowMs + ", lastWriteTimeMs = " + lastWriteTimeMs, lastWriteTimeMs <= nowMs);
        // last write time should be larger or equal than the now - MARGIN_OF_ERROR_MS
        assertTrue("nowMs = " + nowMs + ", lastWriteTimeMs = " + lastWriteTimeMs,
                lastWriteTimeMs >= nowMs - MARGIN_OF_ERROR_MS);
    }

    @Test
    public void lastWriteTime_whenNothingWritten() {
        TcpIpConnection c = connect(connManagerA, addressB);

        long result1 = c.lastWriteTimeMillis();
        long result2 = c.lastWriteTimeMillis();

        assertEquals(result1, result2);
    }

    // we check the lastReadTimeMillis by sending a packet on the local connection, and
    // on the remote side we check the if the lastReadTime is updated
    @Test
    public void lastReadTimeMillis() {
        TcpIpConnection connAB = connect(connManagerA, addressB);
        TcpIpConnection connBA = connect(connManagerB, addressA);

        // we need to sleep some so that the lastReadTime of the connection gets nice and old.
        // we need this so we can determine the lastReadTime got updated
        sleepSeconds(LAST_READ_WRITE_SLEEP_SECONDS);

        Packet packet = new Packet(serializationService.toBytes("foo"));

        connAB.write(packet);

        // wait for the packet to get read
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(1, packetsB.size());
                System.out.println("Packet processed");
            }
        });

        long lastReadTimeMs = connBA.lastReadTimeMillis();
        long nowMs = currentTimeMillis();

        // make sure that the lastRead time is within the given MARGIN_OF_ERROR_MS

        // last read time should be equal or smaller than now
        assertTrue("nowMs = " + nowMs + ", lastReadTimeMs = " + lastReadTimeMs, lastReadTimeMs <= nowMs);
        // last read time should be larger or equal than the now - MARGIN_OF_ERROR_MS
        assertTrue("nowMs = " + nowMs + ", lastReadTimeMs = " + lastReadTimeMs, lastReadTimeMs >= nowMs - MARGIN_OF_ERROR_MS);
    }

    @Test
    public void lastReadTime_whenNothingWritten() {
        TcpIpConnection c = connect(connManagerA, addressB);

        long result1 = c.lastReadTimeMillis();
        long result2 = c.lastReadTimeMillis();

        assertEquals(result1, result2);
    }

    @Test
    public void write_whenNotAlive() {
        TcpIpConnection c = connect(connManagerA, addressB);
        c.close(null, null);

        Packet packet = new Packet(serializationService.toBytes("foo"));

        boolean result = c.write(packet);

        assertFalse(result);
    }

    @Test
    public void getInetAddress() {
        TcpIpConnection c = connect(connManagerA, addressB);

        InetAddress result = c.getInetAddress();

        assertEquals(c.getChannel().socket().getInetAddress(), result);
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

        assertEquals(c.getChannel().socket().getPort(), result);
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
