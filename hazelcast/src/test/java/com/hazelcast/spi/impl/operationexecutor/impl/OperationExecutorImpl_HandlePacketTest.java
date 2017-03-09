/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.operationexecutor.impl;

import com.hazelcast.nio.Packet;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;
import com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.nio.Packet.FLAG_OP_RESPONSE;
import static org.junit.Assert.assertTrue;

/**
 * Tests {@link OperationExecutorImpl#handle(Packet)}.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class OperationExecutorImpl_HandlePacketTest extends OperationExecutorImpl_AbstractTest {

    @Test(expected = NullPointerException.class)
    public void test_whenNullPacket() {
        initExecutor();

        executor.handle(null);
    }

    @Test
    public void test_whenResponsePacket() {
        initExecutor();

        final NormalResponse normalResponse = new NormalResponse(null, 1, 0, false);
        final Packet packet = new Packet(serializationService.toBytes(normalResponse), 0)
                .setPacketType(Packet.Type.OPERATION)
                .raiseFlags(FLAG_OP_RESPONSE);
        executor.handle(packet);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                DummyResponsePacketHandler responsePacketHandler
                        = (DummyResponsePacketHandler) OperationExecutorImpl_HandlePacketTest.this.responsePacketHandler;
                responsePacketHandler.packets.contains(packet);
                responsePacketHandler.responses.contains(normalResponse);
            }
        });
    }

    @Test
    public void test_whenPartitionSpecificOperationPacket() {
        initExecutor();

        final DummyOperation operation = new DummyOperation(0);
        final Packet packet = new Packet(serializationService.toBytes(operation), operation.getPartitionId())
                .setPacketType(Packet.Type.OPERATION);
        executor.handle(packet);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                OperationRunner[] partitionHandlers = executor.getPartitionOperationRunners();
                DummyOperationRunner handler = (DummyOperationRunner) partitionHandlers[operation.getPartitionId()];
                assertContains(handler.packets, packet);
            }
        });
    }

    @Test
    public void test_whenGenericOperationPacket() {
        initExecutor();

        final DummyOperation operation = new DummyOperation(Operation.GENERIC_PARTITION_ID);
        final Packet packet = new Packet(serializationService.toBytes(operation), operation.getPartitionId())
                .setPacketType(Packet.Type.OPERATION);
        executor.handle(packet);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                OperationRunner[] genericHandlers = executor.getGenericOperationRunners();
                boolean found = false;
                for (OperationRunner h : genericHandlers) {
                    DummyOperationRunner dummyOperationHandler = (DummyOperationRunner) h;
                    if (dummyOperationHandler.packets.contains(packet)) {
                        found = true;
                        break;
                    }
                }
                assertTrue("Packet is not found on any of the generic handlers", found);
            }
        });
    }
}
