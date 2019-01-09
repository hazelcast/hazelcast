/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.util;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.internal.networking.HandlerStatus;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.function.Supplier;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.hazelcast.internal.networking.HandlerStatus.CLEAN;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientMessageEncoderTest extends HazelcastTestSupport {

    private ClientMessageEncoder encoder;
    private ClientMessageSupplier src;

    @Before
    public void setup() {
        encoder = new ClientMessageEncoder();
        src = new ClientMessageSupplier();
    }

    @Test
    public void test() {
        ClientMessage message = ClientMessage.createForEncode(1000)
                .setPartitionId(10)
                .setMessageType(1);

        src.queue.add(message);
        encoder.src(src);

        ByteBuffer dst = ByteBuffer.allocate(1000);
        dst.flip();
        encoder.dst(dst);

        HandlerStatus result = encoder.onWrite();

        assertEquals(CLEAN, result);
        ClientMessage clone = ClientMessage.createForDecode(new SafeBuffer(dst.array()), 0);

        assertEquals(message.getPartitionId(), clone.getPartitionId());
        assertEquals(message.getMessageType(), clone.getMessageType());
    }

    public static class ClientMessageSupplier implements Supplier<ClientMessage> {
        public Queue<ClientMessage> queue = new LinkedBlockingQueue<ClientMessage>();

        @Override
        public ClientMessage get() {
            return queue.poll();
        }
    }
}
