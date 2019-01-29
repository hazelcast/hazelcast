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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.PacketIOHelper;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class PacketEncoderTest extends HazelcastTestSupport {

    private InternalSerializationService serializationService;
    private PacketEncoder writeHandler;

    @Before
    public void setup() {
        serializationService = new DefaultSerializationServiceBuilder().build();
        writeHandler = new PacketEncoder();
    }

    @Test
    public void test() throws Exception {
        Packet packet = new Packet(serializationService.toBytes("foobar"));
        ByteBuffer bb = ByteBuffer.allocate(1000);
        boolean result = writeHandler.onWrite(packet, bb);

        assertTrue(result);

        // now we read out the bb and check if we can find the written packet.
        bb.flip();
        Packet resultPacket = new PacketIOHelper().readFrom(bb);
        assertEquals(packet, resultPacket);
    }
}
