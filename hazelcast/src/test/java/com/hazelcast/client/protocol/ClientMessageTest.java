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

package com.hazelcast.client.protocol;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

/**
 * ClientMessage Tests of Flyweight functionality
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMessageTest {

    @Test
    public void shouldEncodeAndDecodeClientMessageCorrectly() {
        ClientMessage cmEncode = ClientMessage.createForEncode();

        cmEncode.add(new ClientMessage.Frame(new byte[50], ClientMessage.DEFAULT_FLAGS));
        cmEncode.setMessageType(1)
                .setCorrelationId(0x1234567812345678L)
                .setPartitionId(0x11223344);

        ClientMessage cmDecode = ClientMessage.createForDecode(cmEncode);

        assertEquals(cmEncode.getMessageType(), cmDecode.getMessageType());
        assertEquals(cmEncode.getHeaderFlags(), cmDecode.getHeaderFlags());
        assertEquals(cmEncode.getCorrelationId(), cmDecode.getCorrelationId());
        assertEquals(cmEncode.getPartitionId(), cmDecode.getPartitionId());
        assertEquals(cmEncode.getFrameLength(), cmDecode.getFrameLength());
    }

    @Test
    public void test_empty_toString() {
        ClientMessage.createForEncode().toString();
    }

    @Test
    public void testCopyClientMessageWithSharingRestOfTheFrames() {
        ClientMessage clientMessage = ClientMessage.createForEncode();

        clientMessage.add(new ClientMessage.Frame(new byte[50], ClientMessage.DEFAULT_FLAGS));
        clientMessage.setMessageType(1)
                .setCorrelationId(0x1234567812345678L)
                .setPartitionId(0x11223344);


        clientMessage.setRetryable(true);
        clientMessage.setAcquiresResource(true);
        clientMessage.setOperationName("operationName");
        clientMessage.add(new ClientMessage.Frame(new byte[20], ClientMessage.IS_FINAL_FLAG));

        int newCorrelationId = 2;
        ClientMessage copyMessage = clientMessage.copyWithNewCorrelationId(newCorrelationId);
        assertEquals(clientMessage.getMessageType(), copyMessage.getMessageType());
        assertEquals(clientMessage.get(1), copyMessage.get(1));
        assertEquals(newCorrelationId, copyMessage.getCorrelationId());
        assertEquals(clientMessage.getPartitionId(), copyMessage.getPartitionId());
        assertEquals(clientMessage.isRetryable(), copyMessage.isRetryable());
        assertEquals(clientMessage.acquiresResource(), copyMessage.acquiresResource());
        assertEquals(clientMessage.getOperationName(), copyMessage.getOperationName());
    }
}
