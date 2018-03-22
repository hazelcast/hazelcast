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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.config.Config;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.function.Consumer;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.nio.Packet.FLAG_OP_RESPONSE;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class AsyncInboundResponseHandlerTest extends HazelcastTestSupport {

    private Consumer<Packet> responsePacketHandler;
    private AsyncInboundResponseHandler asyncHandler;
    private InternalSerializationService serializationService;

    @Before
    public void setup() {
        ILogger logger = Logger.getLogger(getClass());
        responsePacketHandler = mock(Consumer.class);
        asyncHandler = new AsyncInboundResponseHandler(getClass().getClassLoader(), "hz", logger,
                responsePacketHandler, new HazelcastProperties(new Config()));
        asyncHandler.start();
        serializationService = new DefaultSerializationServiceBuilder().build();
    }

    @Test
    public void whenNoProblemPacket() {
        final Packet packet = new Packet(serializationService.toBytes(new NormalResponse("foo", 1, 0, false)))
                .setPacketType(Packet.Type.OPERATION)
                .raiseFlags(FLAG_OP_RESPONSE);
        asyncHandler.accept(packet);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                verify(responsePacketHandler).accept(packet);
            }
        });
    }

    @Test
    public void whenPacketThrowsException() {
        final Packet badPacket = new Packet(serializationService.toBytes(new NormalResponse("bad", 1, 0, false)))
                .setPacketType(Packet.Type.OPERATION)
                .raiseFlags(FLAG_OP_RESPONSE);

        final Packet goodPacket = new Packet(serializationService.toBytes(new NormalResponse("good", 1, 0, false)))
                .setPacketType(Packet.Type.OPERATION)
                .raiseFlags(FLAG_OP_RESPONSE);

        doThrow(new ExpectedRuntimeException()).when(responsePacketHandler).accept(badPacket);

        asyncHandler.accept(badPacket);
        asyncHandler.accept(goodPacket);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                verify(responsePacketHandler).accept(goodPacket);
            }
        });
    }

    @Test
    public void whenShutdown() {
        asyncHandler.shutdown();

        // we need to wait for the responseThread to die first.
        assertJoinable(asyncHandler.responseThread);

        final Packet packet = new Packet(serializationService.toBytes(new NormalResponse("foo", 1, 0, false)))
                .setPacketType(Packet.Type.OPERATION)
                .raiseFlags(FLAG_OP_RESPONSE);

        asyncHandler.accept(packet);

        assertTrueFiveSeconds(new AssertTask() {
            @Override
            public void run() throws Exception {
                verifyZeroInteractions(responsePacketHandler);
            }
        });
    }
}
