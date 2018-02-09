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

package com.hazelcast.jet.impl;

import com.hazelcast.jet.impl.execution.ExecutionContext;
import com.hazelcast.jet.impl.execution.SenderTasklet;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledFuture;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.Util.createObjectDataInput;
import static com.hazelcast.jet.impl.util.Util.createObjectDataOutput;
import static com.hazelcast.jet.impl.util.Util.getMemberConnection;
import static com.hazelcast.jet.impl.util.Util.getRemoteMembers;
import static com.hazelcast.jet.impl.util.Util.idToString;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static com.hazelcast.nio.Packet.FLAG_JET_FLOW_CONTROL;
import static com.hazelcast.nio.Packet.FLAG_URGENT;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class Networking {
    private static final byte[] EMPTY_BYTES = new byte[0];

    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;
    private final JobExecutionService jobExecutionService;
    private final ScheduledFuture<?> flowControlSender;

    Networking(NodeEngine nodeEngine, JobExecutionService jobExecutionService, int flowControlPeriodMs) {
        this.nodeEngine = (NodeEngineImpl) nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
        this.jobExecutionService = jobExecutionService;
        this.flowControlSender = nodeEngine.getExecutionService().scheduleWithRepetition(
                this::broadcastFlowControlPacket, 0, flowControlPeriodMs, MILLISECONDS);
    }

    void shutdown() {
        flowControlSender.cancel(false);
    }

    void handle(Packet packet) throws IOException {
        if (!packet.isFlagRaised(FLAG_JET_FLOW_CONTROL)) {
            handleStreamPacket(packet);
            return;
        }
        handleFlowControlPacket(packet.getConn().getEndPoint(), packet.toByteArray());
    }

    private void handleStreamPacket(Packet packet) throws IOException {
        BufferObjectDataInput in = createObjectDataInput(nodeEngine, packet.toByteArray());
        long executionId = in.readLong();
        int vertexId = in.readInt();
        int ordinal = in.readInt();
        ExecutionContext executionContext = jobExecutionService.getExecutionContext(executionId);
        executionContext.handlePacket(vertexId, ordinal, packet.getConn().getEndPoint(), in);
    }

    public static byte[] createStreamPacketHeader(NodeEngine nodeEngine, long executionId,
                                                  int destinationVertexId, int ordinal) {
        try (BufferObjectDataOutput out = createObjectDataOutput(nodeEngine)) {
            out.writeLong(executionId);
            out.writeInt(destinationVertexId);
            out.writeInt(ordinal);
            return out.toByteArray();
        } catch (IOException e) {
            throw sneakyThrow(e);
        }
    }

    private void broadcastFlowControlPacket() {
        try {
            getRemoteMembers(nodeEngine).forEach(member -> uncheckRun(() -> {
                final byte[] packetBuf = createFlowControlPacket(member);
                if (packetBuf.length == 0) {
                    return;
                }
                Connection conn = getMemberConnection(nodeEngine, member);
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

    private byte[] createFlowControlPacket(Address member) throws IOException {
        try (BufferObjectDataOutput out = createObjectDataOutput(nodeEngine)) {
            final boolean[] hasData = {false};
            Map<Long, ExecutionContext> executionContexts = jobExecutionService.getExecutionContexts();
            out.writeInt(executionContexts.size());
            executionContexts.forEach((execId, exeCtx) -> uncheckRun(() -> {
                if (!exeCtx.hasParticipant(member)) {
                    return;
                }
                out.writeLong(execId);
                out.writeInt(exeCtx.receiverMap().values().stream().mapToInt(Map::size).sum());
                exeCtx.receiverMap().forEach((vertexId, ordinalToSenderToTasklet) ->
                        ordinalToSenderToTasklet.forEach((ordinal, senderToTasklet) -> uncheckRun(() -> {
                            out.writeInt(vertexId);
                            out.writeInt(ordinal);
                            out.writeInt(senderToTasklet.get(member).updateAndGetSendSeqLimitCompressed());
                            hasData[0] = true;
                        })));
            }));
            return hasData[0] ? out.toByteArray() : EMPTY_BYTES;
        }
    }

    private void handleFlowControlPacket(Address fromAddr, byte[] packet) throws IOException {
        try (BufferObjectDataInput in = createObjectDataInput(nodeEngine, packet)) {
            final int executionCtxCount = in.readInt();
            for (int j = 0; j < executionCtxCount; j++) {
                final long executionId = in.readLong();
                final Map<Integer, Map<Integer, Map<Address, SenderTasklet>>> senderMap
                        = jobExecutionService.getSenderMap(executionId);

                if (senderMap == null) {
                    logMissingExeCtx(executionId);
                    continue;
                }
                final int flowCtlMsgCount = in.readInt();
                for (int k = 0; k < flowCtlMsgCount; k++) {
                    int destVertexId = in.readInt();
                    int destOrdinal = in.readInt();
                    int sendSeqLimitCompressed = in.readInt();
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
