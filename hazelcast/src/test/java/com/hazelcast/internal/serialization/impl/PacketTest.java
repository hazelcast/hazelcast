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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.nio.Packet;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.nio.Packet.FLAG_4_0;
import static com.hazelcast.internal.nio.Packet.FLAG_OP_CONTROL;
import static com.hazelcast.internal.nio.Packet.FLAG_URGENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PacketTest {

    @Test
    public void isFlag_4_xSet() {
        byte[] payload = {};
        Packet packet = new Packet();
        Packet packet2 = new Packet(payload);
        Packet packet3 = new Packet(payload, 1);

        assertTrue(packet.isFlagRaised(FLAG_4_0));
        assertTrue(packet2.isFlagRaised(FLAG_4_0));
        assertTrue(packet3.isFlagRaised(FLAG_4_0));
    }

    @Test
    public void raiseFlags() {
        Packet packet = new Packet();
        packet.raiseFlags(FLAG_URGENT);

        assertEquals(FLAG_4_0 | FLAG_URGENT, packet.getFlags());
    }

    @Test
    public void setPacketType() {
        Packet packet = new Packet();
        for (Packet.Type type : Packet.Type.values()) {
            packet.setPacketType(type);
            assertSame(type, packet.getPacketType());
        }
    }

    @Test
    public void isFlagSet() {
        Packet packet = new Packet();
        packet.setPacketType(Packet.Type.OPERATION);
        packet.raiseFlags(FLAG_URGENT);

        assertSame(Packet.Type.OPERATION, packet.getPacketType());
        assertTrue(packet.isFlagRaised(FLAG_URGENT));
        assertFalse(packet.isFlagRaised(FLAG_OP_CONTROL));
    }

    @Test
    public void resetFlagsTo() {
        Packet packet = new Packet().setPacketType(Packet.Type.OPERATION);
        packet.resetFlagsTo(FLAG_URGENT);

        assertSame(Packet.Type.NULL, packet.getPacketType());
        assertEquals(FLAG_URGENT, packet.getFlags());
    }
}
