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

package com.hazelcast.jet.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.impl.execution.ExecutionContext;
import com.hazelcast.jet.impl.execution.ExecutionContext.SenderReceiverKey;
import com.hazelcast.jet.impl.execution.ReceiverTasklet;
import com.hazelcast.jet.impl.execution.SenderTasklet;
import com.hazelcast.jet.impl.serialization.MemoryReader;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledFuture;

import static com.hazelcast.internal.nio.Packet.FLAG_JET_FLOW_CONTROL;
import static com.hazelcast.internal.nio.Packet.FLAG_URGENT;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.ImdgUtil.createObjectDataInput;
import static com.hazelcast.jet.impl.util.ImdgUtil.createObjectDataOutput;
import static com.hazelcast.jet.impl.util.ImdgUtil.getMemberConnection;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class Networking {

    public static final int PACKET_HEADER_SIZE = 16;
    private static final int FLOW_PACKET_INITIAL_SIZE = 128;
    private static final int TERMINAL_VERTEX_ID = -1;
    private static final long TERMINAL_EXECUTION_ID = Long.MIN_VALUE;

    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;
    private final JobExecutionService jobExecutionService;
    private final ScheduledFuture<?> flowControlSender;
    private final MemoryReader memoryReader;

    private int lastFlowPacketSize;

    Networking(NodeEngine nodeEngine, JobExecutionService jobExecutionService, int flowControlPeriodMs) {
        this.nodeEngine = (NodeEngineImpl) nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
        this.jobExecutionService = jobExecutionService;
        this.flowControlSender = nodeEngine.getExecutionService().scheduleWithRepetition(
                this::broadcastFlowControlPacket, 0, flowControlPeriodMs, MILLISECONDS);
        this.memoryReader =
                MemoryReader.create(((InternalSerializationService) nodeEngine.getSerializationService()).getByteOrder());
        this.lastFlowPacketSize = FLOW_PACKET_INITIAL_SIZE;
    }

    void shutdown() {
        flowControlSender.cancel(false);
    }

    void handle(Packet packet) throws IOException {
        if (packet.isFlagRaised(FLAG_JET_FLOW_CONTROL)) {
            handleFlowControlPacket(packet.getConn().getRemoteAddress(), packet.toByteArray());
        } else {
            handleStreamPacket(packet);
        }
    }

    private void handleStreamPacket(Packet packet) {
        byte[] payload = packet.toByteArray();

        long executionId = memoryReader.readLong(payload, 0);
        int vertexId = memoryReader.readInt(payload, Long.BYTES);
        int ordinal = memoryReader.readInt(payload, Long.BYTES + Integer.BYTES);

        ExecutionContext executionContext = jobExecutionService.getOrCreateExecutionContext(executionId);
        if (executionContext != null) {
            executionContext.handlePacket(vertexId, ordinal, packet.getConn().getRemoteAddress(), payload);
        }
    }

    public static byte[] createStreamPacketHeader(NodeEngine nodeEngine,
                                                  long executionId, int destinationVertexId, int ordinal) {
        try (BufferObjectDataOutput output = createObjectDataOutput(nodeEngine, PACKET_HEADER_SIZE)) {
            output.writeLong(executionId);
            output.writeInt(destinationVertexId);
            output.writeInt(ordinal);
            return output.toByteArray();
        } catch (IOException e) {
            throw sneakyThrow(e);
        }
    }

    private void broadcastFlowControlPacket() {
        try {
            final Map<Address, byte[]> packets = createFlowControlPacket();

            for (Entry<Address, byte[]> en : packets.entrySet()) {
                Connection conn = getMemberConnection(nodeEngine, en.getKey());
                if (conn != null) {
                    conn.write(new Packet(en.getValue())
                            .setPacketType(Packet.Type.JET)
                            .raiseFlags(FLAG_URGENT | FLAG_JET_FLOW_CONTROL));
                }
            }
        } catch (Throwable t) {
            logger.severe("Flow-control packet broadcast failed", t);
        }
    }

    private Map<Address, byte[]> createFlowControlPacket() throws IOException {
        class MemberData {
            final BufferObjectDataOutput output = createObjectDataOutput(nodeEngine, lastFlowPacketSize);
            final Connection memberConnection;
            Long startedExecutionId;

            MemberData(Address address) {
                memberConnection = getMemberConnection(nodeEngine, address);
            }
        }

        Map<Address, MemberData> res = new HashMap<>();
        for (ExecutionContext execCtx : jobExecutionService.getExecutionContexts()) {
            Map<SenderReceiverKey, ReceiverTasklet> receiverMap = execCtx.receiverMap();
            if (receiverMap == null) {
                continue;
            }
            for (Entry<SenderReceiverKey, ReceiverTasklet> en : receiverMap.entrySet()) {
                assert !en.getKey().address.equals(nodeEngine.getThisAddress());
                MemberData md = res.computeIfAbsent(en.getKey().address, address -> new MemberData(address));
                if (md.startedExecutionId == null) {
                    md.startedExecutionId = execCtx.executionId();
                    md.output.writeLong(md.startedExecutionId);
                }
                assert en.getKey().vertexId != TERMINAL_VERTEX_ID;
                md.output.writeInt(en.getKey().vertexId);
                md.output.writeInt(en.getKey().ordinal);
                md.output.writeInt(en.getValue().updateAndGetSendSeqLimitCompressed(md.memberConnection));
            }
            for (MemberData md : res.values()) {
                if (md.startedExecutionId != null) {
                    // write a mark to terminate values for an execution
                    md.output.writeInt(TERMINAL_VERTEX_ID);
                    md.startedExecutionId = null;
                }
            }
        }
        for (MemberData md : res.values()) {
            assert md.output.position() > 0;
            // write a mark to terminate all executions
            // Execution IDs are generated using Flake ID generator and those are >0 normally, we
            // use MIN_VALUE as a terminator.
            md.output.writeLong(TERMINAL_EXECUTION_ID);
        }

        // finalize the packets
        int maxSize = 0;
        for (Entry<Address, MemberData> entry : res.entrySet()) {
            byte[] data = entry.getValue().output.toByteArray();
            // we break type safety to avoid creating a new map, we replace the values to a different type in place
            @SuppressWarnings({"unchecked", "rawtypes"})
            Entry<Address, byte[]> entry1 = (Entry) entry;
            entry1.setValue(data);
            if (data.length > maxSize) {
                maxSize = data.length;
            }
        }
        lastFlowPacketSize = maxSize;
        return (Map) res;
    }

    private void handleFlowControlPacket(Address fromAddr, byte[] packet) throws IOException {
        BufferObjectDataInput input = createObjectDataInput(nodeEngine, packet);
        for (;;) {
            final long executionId = input.readLong();
            if (executionId == TERMINAL_EXECUTION_ID) {
                break;
            }
            final Map<SenderReceiverKey, SenderTasklet> senderMap = jobExecutionService.getSenderMap(executionId);
            for (;;) {
                final int vertexId = input.readInt();
                if (vertexId == TERMINAL_VERTEX_ID) {
                    break;
                }
                int ordinal = input.readInt();
                int sendSeqLimitCompressed = input.readInt();
                final SenderTasklet t;
                if (senderMap != null && (t = senderMap.get(new SenderReceiverKey(vertexId, ordinal, fromAddr))) != null) {
                    t.setSendSeqLimitCompressed(sendSeqLimitCompressed);
                }
            }
        }
    }
}
