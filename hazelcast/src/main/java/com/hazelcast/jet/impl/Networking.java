/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.impl.execution.ReceiverTasklet;
import com.hazelcast.jet.impl.execution.SenderTasklet;
import com.hazelcast.jet.impl.serialization.MemoryReader;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ScheduledFuture;

import static com.hazelcast.internal.nio.Packet.FLAG_JET_FLOW_CONTROL;
import static com.hazelcast.internal.nio.Packet.FLAG_URGENT;
import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.ImdgUtil.createObjectDataInput;
import static com.hazelcast.jet.impl.util.ImdgUtil.createObjectDataOutput;
import static com.hazelcast.jet.impl.util.ImdgUtil.getMemberConnection;
import static com.hazelcast.jet.impl.util.ImdgUtil.getRemoteMembers;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class Networking {

    private static final int PACKET_HEADER_SIZE = 16;
    private static final int FLOW_PACKET_INITIAL_SIZE = 128;

    private static final byte[] EMPTY_BYTES = new byte[0];

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
        int offset = 0;

        long executionId = memoryReader.readLong(payload, offset);
        offset += Long.BYTES;
        int vertexId = memoryReader.readInt(payload, offset);
        offset += Integer.BYTES;
        int ordinal = memoryReader.readInt(payload, offset);
        offset += Integer.BYTES;

        ExecutionContext executionContext = jobExecutionService.getExecutionContext(executionId);
        executionContext.handlePacket(vertexId, ordinal, packet.getConn().getRemoteAddress(), payload, offset);
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
            getRemoteMembers(nodeEngine).forEach(member -> uncheckRun(() -> {
                Connection conn = getMemberConnection(nodeEngine, member);
                final byte[] packetBuf = createFlowControlPacket(member, conn);
                if (packetBuf.length == 0) {
                    return;
                }
                if (conn != null) {
                    conn.write(new Packet(packetBuf)
                            .setPacketType(Packet.Type.JET)
                            .raiseFlags(FLAG_URGENT | FLAG_JET_FLOW_CONTROL));
                }
            }));
        } catch (Throwable t) {
            logger.severe("Flow-control packet broadcast failed", t);
        }
    }

    private byte[] createFlowControlPacket(Address member, Connection expectedConnection) throws IOException {
        try (BufferObjectDataOutput output = createObjectDataOutput(nodeEngine, lastFlowPacketSize)) {
            boolean hasData = false;
            Map<Long, ExecutionContext> executionContexts = jobExecutionService.getExecutionContextsFor(member);
            output.writeInt(executionContexts.size());
            for (Entry<Long, ExecutionContext> executionIdAndCtx : executionContexts.entrySet()) {
                output.writeLong(executionIdAndCtx.getKey());
                // dest vertex id --> dest ordinal --> sender addr --> receiver tasklet
                Map<Integer, Map<Integer, Map<Address, ReceiverTasklet>>> receiverMap =
                        executionIdAndCtx.getValue().receiverMap();
                if (receiverMap != null) {
                    output.writeInt(receiverMap.values().stream().mapToInt(Map::size).sum());
                    for (Entry<Integer, Map<Integer, Map<Address, ReceiverTasklet>>> e1 : receiverMap.entrySet()) {
                        int vertexId = e1.getKey();
                        Map<Integer, Map<Address, ReceiverTasklet>> ordinalToMemberToTasklet = e1.getValue();
                        for (Entry<Integer, Map<Address, ReceiverTasklet>> e2 : ordinalToMemberToTasklet.entrySet()) {
                            int ordinal = e2.getKey();
                            Map<Address, ReceiverTasklet> memberToTasklet = e2.getValue();
                            output.writeInt(vertexId);
                            output.writeInt(ordinal);
                            ReceiverTasklet receiverTasklet = memberToTasklet.get(member);
                            output.writeInt(receiverTasklet.updateAndGetSendSeqLimitCompressed(expectedConnection));
                            hasData = true;
                        }
                    }
                }
            }
            if (hasData) {
                byte[] payload = output.toByteArray();
                lastFlowPacketSize = payload.length;
                return payload;
            } else {
                return EMPTY_BYTES;
            }
        }
    }

    private void handleFlowControlPacket(Address fromAddr, byte[] packet) throws IOException {
        try (BufferObjectDataInput input = createObjectDataInput(nodeEngine, packet)) {
            final int executionCtxCount = input.readInt();
            for (int j = 0; j < executionCtxCount; j++) {
                final long executionId = input.readLong();
                final Map<Integer, Map<Integer, Map<Address, SenderTasklet>>> senderMap
                        = jobExecutionService.getSenderMap(executionId);

                if (senderMap == null) {
                    logMissingExeCtx(executionId);
                    continue;
                }
                final int flowCtlMsgCount = input.readInt();
                for (int k = 0; k < flowCtlMsgCount; k++) {
                    int destVertexId = input.readInt();
                    int destOrdinal = input.readInt();
                    int sendSeqLimitCompressed = input.readInt();
                    final SenderTasklet t = Optional.ofNullable(senderMap.get(destVertexId))
                                                    .map(ordinalMap -> ordinalMap.get(destOrdinal))
                                                    .map(addrMap -> addrMap.get(fromAddr))
                                                    .orElse(null);
                    if (t == null) {
                        logMissingSenderTasklet(destVertexId, destOrdinal);
                        return;
                    }
                    t.setSendSeqLimitCompressed(sendSeqLimitCompressed);
                }
            }
        }
    }

    private void logMissingExeCtx(long executionId) {
        if (logger.isFinestEnabled()) {
            logger.finest("Ignoring flow control message applying to non-existent execution context "
                    + idToString(executionId));
        }
    }

    private void logMissingSenderTasklet(int destVertexId, int destOrdinal) {
        if (logger.isFinestEnabled()) {
            logger.finest(String.format(
                    "Ignoring flow control message applying to non-existent sender tasklet (%d, %d)",
                    destVertexId, destOrdinal));
        }
    }
}
