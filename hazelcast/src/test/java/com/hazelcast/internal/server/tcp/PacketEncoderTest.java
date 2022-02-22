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

import com.hazelcast.internal.networking.HandlerStatus;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.nio.PacketIOHelper;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

import static com.hazelcast.internal.networking.HandlerStatus.CLEAN;
import static com.hazelcast.internal.networking.HandlerStatus.DIRTY;
import static com.hazelcast.internal.util.JVMUtil.upcast;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PacketEncoderTest extends HazelcastTestSupport {

    private InternalSerializationService serializationService;
    private PacketEncoder encoder;

    @Before
    public void setup() {
        serializationService = new DefaultSerializationServiceBuilder().build();
        encoder = new PacketEncoder();
    }

    @Test
    public void whenPacketFullyWritten() {
        final Packet packet = new Packet(serializationService.toBytes("foobar"));
        ByteBuffer dst = ByteBuffer.allocate(1000);
        upcast(dst).flip();

        PacketSupplier src = new PacketSupplier();
        src.queue.add(packet);

        encoder.dst(dst);
        encoder.src(src);

        HandlerStatus result = encoder.onWrite();

        assertEquals(CLEAN, result);

        // now we read out the dst and check if we can find the written packet.
        Packet resultPacket = new PacketIOHelper().readFrom(dst);
        assertEquals(packet, resultPacket);
    }

    @Test
    public void whenNotEnoughSpace() {
        final Packet packet = new Packet(serializationService.toBytes(new byte[2000]));
        ByteBuffer dst = ByteBuffer.allocate(1000);
        upcast(dst).flip();

        PacketSupplier src = new PacketSupplier();
        src.queue.add(packet);

        encoder.dst(dst);
        encoder.src(src);

        HandlerStatus result = encoder.onWrite();

        assertEquals(DIRTY, result);
    }

    static class PacketSupplier implements Supplier<Packet> {
        Queue<Packet> queue = new LinkedBlockingQueue<Packet>();

        @Override
        public Packet get() {
            return queue.poll();
        }
    }
}
