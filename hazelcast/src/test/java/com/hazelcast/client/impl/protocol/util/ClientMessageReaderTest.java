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

package com.hazelcast.client.impl.protocol.util;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.ClientMessageReader;
import com.hazelcast.client.impl.protocol.ClientMessageWriter;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.ByteBuffer;
import java.util.Random;

import static com.hazelcast.internal.util.JVMUtil.upcast;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMessageReaderTest {

    private final Random random = new Random();

    @Test
    public void testReadSingleFrameMessage() {
        ClientMessage.Frame frame = createFrameWithRandomBytes(42);

        ClientMessage message = ClientMessage.createForEncode();
        message.add(frame);

        ByteBuffer buffer = writeToBuffer(message);

        ClientMessageReader reader = new ClientMessageReader(-1);
        assertTrue(reader.readFrom(buffer, true));

        ClientMessage messageRead = reader.getClientMessage();
        ClientMessage.ForwardFrameIterator iterator = messageRead.frameIterator();

        assertTrue(iterator.hasNext());
        ClientMessage.Frame frameRead = iterator.next();
        assertArrayEquals(frame.content, frameRead.content);

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testReadMultiFrameMessage() {
        ClientMessage.Frame frame1 = createFrameWithRandomBytes(10);
        ClientMessage.Frame frame2 = createFrameWithRandomBytes(20);
        ClientMessage.Frame frame3 = createFrameWithRandomBytes(30);

        ClientMessage message = ClientMessage.createForEncode();
        message.add(frame1);
        message.add(frame2);
        message.add(frame3);

        ByteBuffer buffer = writeToBuffer(message);

        ClientMessageReader reader = new ClientMessageReader(-1);
        assertTrue(reader.readFrom(buffer, true));

        ClientMessage messageRead = reader.getClientMessage();
        ClientMessage.ForwardFrameIterator iterator = messageRead.frameIterator();

        assertTrue(iterator.hasNext());
        ClientMessage.Frame frameRead = iterator.next();
        assertArrayEquals(frame1.content, frameRead.content);

        assertTrue(iterator.hasNext());
        frameRead = iterator.next();
        assertArrayEquals(frame2.content, frameRead.content);

        assertTrue(iterator.hasNext());
        frameRead = iterator.next();
        assertArrayEquals(frame3.content, frameRead.content);

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testReadFramesInMultipleCallsToReadFrom() {
        ClientMessage.Frame frame = createFrameWithRandomBytes(1000);

        ClientMessage message = ClientMessage.createForEncode();
        message.add(frame);

        ByteBuffer buffer = writeToBuffer(message);

        byte[] part1 = new byte[750];
        buffer.get(part1, 0, 750);
        byte[] part2 = new byte[buffer.remaining()];
        buffer.get(part2);

        ByteBuffer part1Buffer = ByteBuffer.wrap(part1);
        ByteBuffer part2Buffer = ByteBuffer.wrap(part2);

        ClientMessageReader reader = new ClientMessageReader(-1);
        assertFalse(reader.readFrom(part1Buffer, true));
        assertTrue(reader.readFrom(part2Buffer, true));

        ClientMessage messageRead = reader.getClientMessage();
        ClientMessage.ForwardFrameIterator iterator = messageRead.frameIterator();

        assertTrue(iterator.hasNext());
        ClientMessage.Frame frameRead = iterator.next();
        assertArrayEquals(frame.content, frameRead.content);

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testReadFramesInMultipleCallsToReadFrom_whenLastPieceIsSmall() {
        ClientMessage.Frame frame = createFrameWithRandomBytes(1000);

        ClientMessage message = ClientMessage.createForEncode();
        message.add(frame);

        ByteBuffer buffer = writeToBuffer(message);

        byte[] part1 = new byte[750];
        buffer.get(part1, 0, 750);
        byte[] part2 = new byte[252];
        buffer.get(part2, 0, 252);

        // Message Length = 1000 + 6 bytes
        // part1 = 750, part2 = 252, part3 = 4 bytes
        byte[] part3 = new byte[buffer.remaining()];
        buffer.get(part3);

        ByteBuffer part1Buffer = ByteBuffer.wrap(part1);
        ByteBuffer part2Buffer = ByteBuffer.wrap(part2);
        ByteBuffer part3Buffer = ByteBuffer.wrap(part3);

        ClientMessageReader reader = new ClientMessageReader(-1);
        assertFalse(reader.readFrom(part1Buffer, true));
        assertFalse(reader.readFrom(part2Buffer, true));
        assertTrue(reader.readFrom(part3Buffer, true));

        ClientMessage messageRead = reader.getClientMessage();
        ClientMessage.ForwardFrameIterator iterator = messageRead.frameIterator();

        assertTrue(iterator.hasNext());
        ClientMessage.Frame frameRead = iterator.next();
        assertArrayEquals(frame.content, frameRead.content);

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testRead_whenTheFrameLengthAndFlagsNotReceivedAtFirst() {
        ClientMessage.Frame frame = createFrameWithRandomBytes(100);

        ClientMessage message = ClientMessage.createForEncode();
        message.add(frame);

        ByteBuffer buffer = writeToBuffer(message);
        int capacity = buffer.capacity();
        // Set limit to a small value so that we can simulate
        // that the frame length and flags are not read yet.
        upcast(buffer).limit(4);

        ClientMessageReader reader = new ClientMessageReader(-1);

        // should not be able to read with just 4 bytes of data
        assertFalse(reader.readFrom(buffer, true));

        upcast(buffer).limit(capacity);

        // should be able to read when the rest of the data comes
        assertTrue(reader.readFrom(buffer, true));

        ClientMessage messageRead = reader.getClientMessage();
        ClientMessage.ForwardFrameIterator iterator = messageRead.frameIterator();

        assertTrue(iterator.hasNext());
        ClientMessage.Frame frameRead = iterator.next();
        assertArrayEquals(frame.content, frameRead.content);

        assertFalse(iterator.hasNext());
    }

    private ClientMessage.Frame createFrameWithRandomBytes(int contentLength) {
        byte[] content = new byte[contentLength];
        random.nextBytes(content);
        return new ClientMessage.Frame(content);
    }

    private ByteBuffer writeToBuffer(ClientMessage message) {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[message.getFrameLength()]);
        ClientMessageWriter writer = new ClientMessageWriter();
        writer.writeTo(buffer, message);
        upcast(buffer).flip();
        return buffer;
    }
}
