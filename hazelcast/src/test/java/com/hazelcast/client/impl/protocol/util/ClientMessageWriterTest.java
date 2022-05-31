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
import com.hazelcast.client.impl.protocol.ClientMessageWriter;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.ByteBuffer;

import static com.hazelcast.client.impl.protocol.ClientMessage.DEFAULT_FLAGS;
import static com.hazelcast.client.impl.protocol.ClientMessage.SIZE_OF_FRAME_LENGTH_AND_FLAGS;
import static com.hazelcast.client.impl.protocol.ClientMessage.createForDecode;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMessageWriterTest {

    @Test
    public void testWriteAttemptToInsufficientSpaceRemaining() {
        ClientMessage.Frame frame = new ClientMessage.Frame(new byte[100], DEFAULT_FLAGS);
        ClientMessage message = createForDecode(frame);
        ByteBuffer buffer = ByteBuffer.allocate(50);
        ClientMessageWriter clientMessageWriter = new ClientMessageWriter();

        assertFalse(clientMessageWriter.writeTo(buffer, message));

    }

    @Test
    public void testWriteAttemptToInsufficientSpaceRemaining_spaceLeftIsLessThanFrameLengthAndFlags() {
        ClientMessage.Frame frame = new ClientMessage.Frame(new byte[100], DEFAULT_FLAGS);
        ClientMessage message = createForDecode(frame);
        ByteBuffer buffer = ByteBuffer.allocate(SIZE_OF_FRAME_LENGTH_AND_FLAGS - 1);
        ClientMessageWriter clientMessageWriter = new ClientMessageWriter();

        assertFalse(clientMessageWriter.writeTo(buffer, message));

    }
}
