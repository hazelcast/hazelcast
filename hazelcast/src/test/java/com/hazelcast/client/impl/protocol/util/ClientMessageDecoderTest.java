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
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.nio.Connection;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.function.Consumer;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.ByteBuffer;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientMessageDecoderTest {

    private Consumer<ClientMessage> messageConsumer;
    private Connection connection;
    private SwCounter counter;
    private ClientMessageDecoder decoder;

    @Before
    public void setup() {
        messageConsumer = mock(Consumer.class);
        connection = mock(Connection.class);
        counter = SwCounter.newSwCounter();
        connection = mock(Connection.class);
        decoder = new ClientMessageDecoder(connection, messageConsumer);
        //decoder.setNormalPacketsRead(counter);
    }

    @Test
    public void test() {
        ClientMessage message = ClientMessage.createForEncode(1000)
                .setPartitionId(10)
                .setMessageType(1)
                .setCorrelationId(1)
                .addFlag(ClientMessage.BEGIN_AND_END_FLAGS);

        ByteBuffer src = ByteBuffer.allocate(1000);
        message.writeTo(src);

        decoder.src(src);
        decoder.onRead();

        verify(messageConsumer).accept(any(ClientMessage.class));
    }
}
