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

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.Member;
import com.hazelcast.core.MigrationEvent;
import com.hazelcast.core.MigrationListener;
import com.hazelcast.jet.JetEngineConfig;
import com.hazelcast.jet.TopologyChangedException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.CanCancelOperations;
import com.hazelcast.spi.LiveOperations;
import com.hazelcast.spi.LiveOperationsTracker;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.PacketHandler;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.hazelcast.jet.impl.Util.createObjectDataInput;
import static com.hazelcast.jet.impl.Util.createObjectDataOutput;
import static com.hazelcast.jet.impl.Util.getMemberConnection;
import static com.hazelcast.jet.impl.Util.getRemoteMembers;
import static com.hazelcast.jet.impl.Util.sneakyThrow;
import static com.hazelcast.jet.impl.Util.uncheckRun;
import static com.hazelcast.nio.Packet.FLAG_JET_FLOW_CONTROL;
import static com.hazelcast.nio.Packet.FLAG_URGENT;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class JetService implements ManagedService, RemoteService, PacketHandler, LiveOperationsTracker,
        CanCancelOperations {

    public static final String SERVICE_NAME = "hz:impl:jetService";
    static final int FLOW_CONTROL_PERIOD_MS = 100;
    private static final byte[] EMPTY_BYTES = new byte[0];
    final ILogger logger;

    private NodeEngineImpl nodeEngine;

    // Type of variables is CHM and not ConcurrentMap because we rely on specific semantics of computeIfAbsent.
    // ConcurrentMap.computeIfAbsent does not guarantee at most one computation per key.
    private final ConcurrentHashMap<String, EngineContext> engineContexts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Address, Map<Long, AsyncExecutionOperation>> liveOperations = new ConcurrentHashMap<>();

    public JetService(NodeEngine nodeEngine) {
        this.nodeEngine = (NodeEngineImpl) nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
    }

    @Override
    public void init(NodeEngine engine, Properties properties) {
        engine.getExecutionService().scheduleWithRepetition(
                this::broadcastFlowControlPacket, 0, FLOW_CONTROL_PERIOD_MS, MILLISECONDS);
        engine.getPartitionService().addMigrationListener(new CancelJobsMigrationListener());
    }

    @Override
    public void reset() {
    }

    @Override
    public void shutdown(boolean terminate) {
        engineContexts.values().forEach(EngineContext::destroy);
    }

    @Override
    public DistributedObject createDistributedObject(String objectName) {
        return new JetEngineProxyImpl(objectName, nodeEngine, this);

    }

    @Override
    public void destroyDistributedObject(String objectName) {
        EngineContext ec = engineContexts.remove(objectName);
        if (ec != null) {
            ec.destroy();
        }
    }

    @Override
    public void populate(LiveOperations liveOperations) {
        this.liveOperations.entrySet().forEach(entry ->
                entry.getValue().keySet().forEach(callId -> liveOperations.add(entry.getKey(), callId))
        );
    }

    @Override
    public boolean cancelOperation(Address caller, long callId) {
        Optional<AsyncExecutionOperation> operation = Optional.of(liveOperations.get(caller)).map(m -> m.get(callId));
        operation.ifPresent(AsyncExecutionOperation::cancel);
        return operation.isPresent();
    }

    @Override
    public void handle(Packet packet) throws Exception {
        if (!packet.isFlagRaised(FLAG_JET_FLOW_CONTROL)) {
            handleStreamPacket(packet);
            return;
        }
        handleFlowControlPacket(packet.getConn().getEndPoint(), packet.toByteArray());
    }

    public EngineContext getEngineContext(String name) {
        return engineContexts.get(name);
    }

    boolean createContextIfAbsent(String name, JetEngineConfig config) {
        boolean[] isNewContext = new boolean[1];
        engineContexts.computeIfAbsent(name, (key) -> {
            isNewContext[0] = true;
            return new EngineContext(name, nodeEngine, config);
        });
        return isNewContext[0];
    }

    void registerOperation(AsyncExecutionOperation operation) {
        Map<Long, AsyncExecutionOperation> callIds = liveOperations.computeIfAbsent(operation.getCallerAddress(),
                (key) -> new ConcurrentHashMap<>());
        if (callIds.putIfAbsent(operation.getCallId(), operation) != null) {
            throw new IllegalStateException("Duplicate operation during registration of operation=" + operation);
        }
    }

    void deregisterOperation(AsyncExecutionOperation operation) {
        if (liveOperations.get(operation.getCallerAddress()).remove(operation.getCallId()) == null) {
            throw new IllegalStateException("Missing operation during de-registration of operation=" + operation);
        }
    }

    private void handleStreamPacket(Packet packet) throws IOException {
        BufferObjectDataInput in = createObjectDataInput(nodeEngine, packet.toByteArray());
        String engineName = in.readUTF();
        long executionId = in.readLong();
        int vertexId = in.readInt();
        int ordinal = in.readInt();
        engineContexts.get(engineName)
                      .getExecutionContext(executionId)
                      .handlePacket(vertexId, ordinal, packet.getConn().getEndPoint(), in);
    }

    static byte[] createStreamPacketHeader(
            NodeEngine nodeEngine, String jetEngineName, long executionId, int destinationVertexId, int ordinal) {
        ObjectDataOutput out = createObjectDataOutput(nodeEngine);
        try {
            out.writeUTF(jetEngineName);
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
        out.writeInt(engineContexts.size());
        engineContexts.forEach((name, engCtx) -> uncheckRun(() -> {
            out.writeUTF(name);
            out.writeInt(engCtx.executionContexts.size());
            engCtx.executionContexts.forEach((execId, exeCtx) -> uncheckRun(() -> {
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
        }));
        return hasData[0] ? out.toByteArray() : EMPTY_BYTES;
    }

    private void handleFlowControlPacket(Address fromAddr, byte[] packet) throws IOException {
        final ObjectDataInput in = createObjectDataInput(nodeEngine, packet);
        final int engineCtxCount = in.readInt();
        for (int i = 0; i < engineCtxCount; i++) {
            final String engCtxName = in.readUTF();
            EngineContext engCtx = engineContexts.get(engCtxName);
            if (engCtx == null) {
                logMissingEngCtx(engCtxName);
                continue;
            }
            final int executionCtxCount = in.readInt();
            for (int j = 0; j < executionCtxCount; j++) {
                final long exeCtxId = in.readLong();
                final Map<Integer, Map<Integer, Map<Address, SenderTasklet>>> senderMap =
                        Optional.ofNullable(engCtx.executionContexts)
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
    }

    private void logMissingEngCtx(String engCtxName) {
        if (logger.isFinestEnabled()) {
            logger.finest("Ignoring flow control message applying to non-existent engine context "
                    + engCtxName);
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


    private class CancelJobsMigrationListener implements MigrationListener {

        @Override
        public void migrationStarted(MigrationEvent migrationEvent) {
            Set<Address> addresses = nodeEngine.getClusterService().getMembers().stream()
                                               .map(Member::getAddress)
                                               .collect(Collectors.toSet());
            // complete the processors, whose caller is dead, with TopologyChangedException
            liveOperations
                    .entrySet().stream()
                    .filter(e -> !addresses.contains(e.getKey()))
                    .flatMap(e -> e.getValue().values().stream())
                    .forEach(op -> {
                        EngineContext engineContext = engineContexts.get(op.getEngineName());
                        Optional.ofNullable(engineContext)
                                .map(ctx -> ctx.getExecutionContext(op.getExecutionId()))
                                .map(ExecutionContext::getExecutionCompletionStage)
                                .ifPresent(stage -> stage.whenComplete((aVoid, throwable) ->
                                        engineContext.completeExecution(op.getExecutionId(),
                                                new TopologyChangedException("Topology has been changed"))));
                    });
            // send exception result to all operations
            liveOperations.values().forEach(map -> map.values().forEach(
                    op -> op.completeExceptionally(new TopologyChangedException("Topology has been changed"))
            ));
        }

        @Override
        public void migrationCompleted(MigrationEvent migrationEvent) {

        }

        @Override
        public void migrationFailed(MigrationEvent migrationEvent) {

        }
    }

}
