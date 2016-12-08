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
import com.hazelcast.jet.JetEngineConfig;
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
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.jet.impl.Util.createObjectDataInput;
import static com.hazelcast.jet.impl.Util.createObjectDataOutput;
import static com.hazelcast.jet.impl.Util.getMemberConnection;
import static com.hazelcast.jet.impl.Util.getRemoteMembers;
import static com.hazelcast.jet.impl.Util.sneakyThrow;
import static com.hazelcast.jet.impl.Util.uncheckRun;
import static com.hazelcast.nio.Packet.FLAG_JET_FLOW_CONTROL;
import static com.hazelcast.nio.Packet.FLAG_URGENT;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.IntStream.range;

public class JetService implements ManagedService, RemoteService, PacketHandler, LiveOperationsTracker, CanCancelOperations {

    public static final String SERVICE_NAME = "hz:impl:jetService";
    private static final int FLOW_CONTROL_PERIOD_MS = 100;
    private static final byte[] EMPTY_BYTES = new byte[0];
    final ILogger logger;

    private NodeEngineImpl nodeEngine;

    // Type of variables is CHM and not ConcurrentMap because we rely on specific semantics of computeIfAbsent.
    // ConcurrentMap.computeIfAbsent does not guarantee at most one computation per key.
    private ConcurrentHashMap<String, EngineContext> engineContexts = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Address, Map<Long, AsyncOperation>> liveOperations = new ConcurrentHashMap<>();

    public JetService(NodeEngine nodeEngine) {
        this.nodeEngine = (NodeEngineImpl) nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
    }

    @Override
    public void init(NodeEngine engine, Properties properties) {
        engine.getExecutionService().scheduleWithRepetition(
                this::broadcastFlowControlPacket, 0, FLOW_CONTROL_PERIOD_MS, MILLISECONDS);
    }

    @Override
    public void reset() {
    }

    @Override
    public void shutdown(boolean terminate) {
        for (EngineContext engineContext : engineContexts.values()) {
            engineContext.destroy();
        }
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
                entry.getValue().keySet().stream().forEach(callId -> liveOperations.add(entry.getKey(), callId))
        );
    }

    @Override
    public boolean cancelOperation(Address caller, long callId) {
        Optional<AsyncOperation> operation = Optional.of(liveOperations.get(caller)).map(m -> m.get(callId));
        operation.ifPresent(AsyncOperation::cancel);
        return operation.isPresent();
    }

    @Override
    public void handle(Packet packet) throws Exception {
        if (packet.isFlagRaised(FLAG_JET_FLOW_CONTROL)) {
            handleFlowControlPacket(packet.getConn().getEndPoint(), packet.toByteArray());
        } else {
            handleStreamPacket(packet);
        }
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

    void registerOperation(AsyncOperation operation) {
        Map<Long, AsyncOperation> callIds = liveOperations.computeIfAbsent(operation.getCallerAddress(),
                (key) -> new ConcurrentHashMap<>());
        if (callIds.putIfAbsent(operation.getCallId(), operation) != null) {
            throw new IllegalStateException("Duplicate operation during registration of operation=" + operation);
        }
    }

    void deregisterOperation(AsyncOperation operation) {
        if (liveOperations.get(operation.getCallerAddress()).remove(operation.getCallId()) == null) {
            throw new IllegalStateException("Missing operation during de-registration of operation=" + operation);
        }
    }

    private void handleStreamPacket(Packet packet) {
        try {
            BufferObjectDataInput in = createObjectDataInput(nodeEngine, packet.toByteArray());
            String engineName = in.readUTF();
            long executionId = in.readLong();
            int vertexId = in.readInt();
            int ordinal = in.readInt();
            engineContexts.get(engineName)
                          .getExecutionContext(executionId)
                          .handlePacket(vertexId, ordinal, packet.getConn().getEndPoint(), in);
        } catch (IOException e) {
            throw sneakyThrow(e);
        }
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
            getRemoteMembers(nodeEngine).forEach(member -> {
                final byte[] packetBuf = createFlowControlPacket(member);
                if (packetBuf.length == 0) {
                    return;
                }
                Connection conn = getMemberConnection(nodeEngine, member);
                conn.write(new Packet(packetBuf)
                        .setPacketType(Packet.Type.JET)
                        .raiseFlags(FLAG_URGENT | FLAG_JET_FLOW_CONTROL));
            });
        } catch (Throwable t) {
            logger.severe("Flow-control packet broadcast failed", t);
        }
    }

    private byte[] createFlowControlPacket(Address member) {
        final ObjectDataOutput out = createObjectDataOutput(nodeEngine);
        final boolean[] hasData = {false};
        uncheckRun(() -> out.writeInt(engineContexts.size()));
        engineContexts.forEach((name, engCtx) -> uncheckRun(() -> {
            out.writeUTF(name);
            out.writeInt(engCtx.executionContexts.size());
            engCtx.executionContexts.forEach((execId, exeCtx) -> uncheckRun(() -> {
                final int memberId = exeCtx.getMemberId(member);
                out.writeLong(execId);
                out.writeInt(exeCtx.receiverMap().values().stream().mapToInt(Map::size).sum());
                exeCtx.receiverMap().forEach((vertexId, m) ->
                    m.forEach((ordinal, tasklet) -> uncheckRun(() -> {
                        out.writeInt(vertexId);
                        out.writeInt(ordinal);
                        out.writeInt(tasklet.ackedSeq(memberId));
                        hasData[0] = true;
                    }
                )));
            }));
        }));
        return hasData[0] ? out.toByteArray() : EMPTY_BYTES;
    }

    private void handleFlowControlPacket(Address fromAddr, byte[] packet) {
        final ObjectDataInput in = createObjectDataInput(nodeEngine, packet);
        uncheckRun(() ->
            range(0, in.readInt()).forEach(x -> uncheckRun(() -> {
                EngineContext engCtx = engineContexts.get(in.readUTF());
                range(0, in.readInt()).forEach(x2 -> uncheckRun(() -> {
                    final Map<Integer, Map<Integer, Map<Address, SenderTasklet>>> senderMap = engCtx
                            .executionContexts
                            .get(in.readLong())
                            .senderMap();
                    range(0, in.readInt()).forEach(x3 -> uncheckRun(() -> {
                        int destVertexId = in.readInt();
                        int destOrdinal = in.readInt();
                        int ackedSeq = in.readInt();
                        senderMap.get(destVertexId)
                                 .get(destOrdinal)
                                 .get(fromAddr)
                                 .setAckedSeqCompressed(ackedSeq);
                    }));
                }));
            }))
        );
    }
}
