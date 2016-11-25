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

package com.hazelcast.jet2.impl;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.jet2.JetEngineConfig;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.CanCancelOperations;
import com.hazelcast.spi.LiveOperations;
import com.hazelcast.spi.LiveOperationsTracker;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.PacketHandler;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class JetService implements ManagedService, RemoteService, PacketHandler, LiveOperationsTracker, CanCancelOperations {

    public static final String SERVICE_NAME = "hz:impl:jetService";
    public static final Charset CHARSET = Charset.forName("ISO-8859-1");

    private NodeEngineImpl nodeEngine;

    // Type of variables is CHM and not ConcurrentMap because we rely on specific semantics of computeIfAbsent.
    // ConcurrentMap.computeIfAbsent does not guarantee at most one computation per key.
    private ConcurrentHashMap<String, EngineContext> engineContexts = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Address, Map<Long, AsyncOperation>> liveOperations = new ConcurrentHashMap<>();

    public JetService(NodeEngine nodeEngine) {
        this.nodeEngine = (NodeEngineImpl) nodeEngine;
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
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
    public void handle(Packet packet) throws Exception {
        byte[] bytes = packet.toByteArray();
        int offset = 0;
        int length = bytes[offset];
        offset++;
        String engineName = new String(bytes, offset, length, CHARSET);
        offset += length;
        long executionId = Bits.readLongL(bytes, offset);
        offset += Bits.LONG_SIZE_IN_BYTES;
        int vertexId = Bits.readIntL(bytes, offset);
        offset += Bits.INT_SIZE_IN_BYTES;
        int ordinal = Bits.readIntL(bytes, offset);
        offset += Bits.INT_SIZE_IN_BYTES;
        engineContexts.get(engineName)
                      .getExecutionContext(executionId)
                      .handlePacket(vertexId, ordinal, bytes, offset);
    }

    public static byte[] createHeader(String name, long executionId, int destinationVertexId, int ordinal) {
        byte[] nameBytes = name.getBytes(CHARSET);
        int length = Bits.BYTE_SIZE_IN_BYTES + nameBytes.length + Bits.LONG_SIZE_IN_BYTES + Bits.INT_SIZE_IN_BYTES
                + Bits.INT_SIZE_IN_BYTES;
        byte[] headerBytes = new byte[length];
        int offset = 0;
        assert nameBytes.length <= Byte.MAX_VALUE : "Length of encoded name in header exceeds " + Byte.MAX_VALUE;
        headerBytes[offset] = (byte) nameBytes.length;
        offset++;
        System.arraycopy(nameBytes, 0, headerBytes, offset, nameBytes.length);
        offset += nameBytes.length;
        Bits.writeLongL(headerBytes, offset, executionId);
        offset += Bits.LONG_SIZE_IN_BYTES;
        Bits.writeIntL(headerBytes, offset, destinationVertexId);
        offset += Bits.INT_SIZE_IN_BYTES;
        Bits.writeIntL(headerBytes, offset, ordinal);
        return headerBytes;
    }

    boolean createContextIfAbsent(String name, JetEngineConfig config) {
        boolean[] isNewContext = new boolean[1];
        engineContexts.computeIfAbsent(name, (key) -> {
            isNewContext[0] = true;
            return new EngineContext(name, nodeEngine, config);
        });
        return isNewContext[0];
    }

    public EngineContext getEngineContext(String name) {
        return engineContexts.get(name);
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

    @Override
    public boolean cancelOperation(Address caller, long callId) {
        Optional<AsyncOperation> operation = Optional.of(liveOperations.get(caller)).map(m -> m.get(callId));
        operation.ifPresent(AsyncOperation::cancel);
        return operation.isPresent();
    }
}
