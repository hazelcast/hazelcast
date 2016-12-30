/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;

import static com.hazelcast.jet.impl.util.Util.createObjectDataInput;
import static com.hazelcast.jet.impl.util.Util.createObjectDataOutput;
import static com.hazelcast.jet.impl.util.Util.getMemberConnection;
import static com.hazelcast.jet.impl.util.Util.getRemoteMembers;
import static com.hazelcast.jet.impl.util.Util.sneakyThrow;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static com.hazelcast.nio.Packet.FLAG_JET_FLOW_CONTROL;
import static com.hazelcast.nio.Packet.FLAG_URGENT;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class Networking {
    public static final int FLOW_CONTROL_PERIOD_MS = 100;
    private static final byte[] EMPTY_BYTES = new byte[0];

    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;
    private final ScheduledFuture<?> flowControlSender;
    private final ConcurrentHashMap<Long, ExecutionContext> executionContexts;

    Networking(
            NodeEngine nodeEngine, ConcurrentHashMap<Long, ExecutionContext> executionContexts, int flowControlPeriodMs
    ) {
        this.nodeEngine = (NodeEngineImpl) nodeEngine;
        this.executionContexts = executionContexts;
        this.logger = nodeEngine.getLogger(getClass());
        this.flowControlSender = nodeEngine.getExecutionService().scheduleWithRepetition(
                this::broadcastFlowControlPacket, 0, flowControlPeriodMs, MILLISECONDS);
    }

    void destroy() {
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
        executionContexts.get(executionId).handlePacket(vertexId, ordinal, packet.getConn().getEndPoint(), in);
    }

    public static byte[] createStreamPacketHeader(
            NodeEngine nodeEngine, long executionId, int destinationVertexId, int ordinal) {
        ObjectDataOutput out = createObjectDataOutput(nodeEngine);
        try {
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
                conn.write(new Packet(packetBuf)
                        .setPacketType(Packet.Type.JET)
                        .raiseFlags(FLAG_URGENT | FLAG_JET_FLOW_CONTROL));
            }));
        } catch (Throwable t) {
            logger.severe("Flow-control packet broadcast failed", t);
        }
    }

    private byte[] createFlowControlPacket(Address member) throws IOException {
        final ObjectDataOutput out = createObjectDataOutput(nodeEngine);
        final boolean[] hasData = {false};
        out.writeInt(executionContexts.size());
        executionContexts.forEach((execId, exeCtx) -> uncheckRun(() -> {
            final Integer memberId = exeCtx.getMemberId(member);
            if (memberId == null) {
                // The target member is not involved in the job associated with this execution context
                return;
            }
            out.writeLong(execId);
            out.writeInt(exeCtx.receiverMap().values().stream().mapToInt(Map::size).sum());
            exeCtx.receiverMap().forEach((vertexId, m) ->
                    m.forEach((ordinal, tasklet) -> uncheckRun(() -> {
                                out.writeInt(vertexId);
                                out.writeInt(ordinal);
                                out.writeInt(tasklet.updateAndGetSendSeqLimitCompressed(memberId));
                                hasData[0] = true;
                            }
                    )));
        }));
        return hasData[0] ? out.toByteArray() : EMPTY_BYTES;
    }

    private void handleFlowControlPacket(Address fromAddr, byte[] packet) throws IOException {
        final ObjectDataInput in = createObjectDataInput(nodeEngine, packet);

        final int executionCtxCount = in.readInt();
        for (int j = 0; j < executionCtxCount; j++) {
            final long exeCtxId = in.readLong();
            final Map<Integer, Map<Integer, Map<Address, SenderTasklet>>> senderMap =
                    Optional.ofNullable(executionContexts)
                            .map(exeCtxs -> exeCtxs.get(exeCtxId))
                            .map(ExecutionContext::senderMap)
                            .orElse(null);
            if (senderMap == null) {
                logMissingExeCtx(exeCtxId);
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

    private void logMissingExeCtx(long exeCtxId) {
        if (logger.isFinestEnabled()) {
            logger.finest("Ignoring flow control message applying to non-existent execution context "
                    + exeCtxId);
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
