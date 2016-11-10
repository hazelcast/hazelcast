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
import com.hazelcast.spi.LiveOperations;
import com.hazelcast.spi.LiveOperationsTracker;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.PacketHandler;
import com.hazelcast.util.ConcurrencyUtil;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class JetService implements ManagedService, RemoteService, PacketHandler, LiveOperationsTracker {

    public static final String SERVICE_NAME = "hz:impl:jetService";
    public static final Charset CHARSET = Charset.forName("ISO-8859-1");

    private NodeEngineImpl nodeEngine;

    private ConcurrentMap<String, EngineContext> engineContexts = new ConcurrentHashMap<>();
    private ConcurrentMap<Address, Map<Long, Boolean>> liveOperations = new ConcurrentHashMap<>();

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
        return new JetEngineImpl(objectName, nodeEngine, this);

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
        this.liveOperations.entrySet().forEach(entry -> entry.getValue().keySet().stream().forEach(callId ->
                liveOperations.add(entry.getKey(), callId)));
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
                      .handlePacket(vertexId, ordinal, packet.getPartitionId(), bytes, offset);
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

    public void ensureContext(String name, JetEngineConfig config) {
        ConcurrencyUtil.getOrPutSynchronized(engineContexts, name, engineContexts,
                (key) -> new EngineContext(name, nodeEngine, config));
    }

    public EngineContext getEngineContext(String name) {
        return engineContexts.get(name);
    }

    void registerOperation(Address caller, long callId) {
        Map<Long, Boolean> callIds = ConcurrencyUtil.getOrPutSynchronized(liveOperations, caller, liveOperations,
                (key) -> new ConcurrentHashMap<>());
        if (callIds.putIfAbsent(callId, true) != null) {
            throw new IllegalStateException("Duplicate operation for caller=" + caller + " callId=" + callId);
        }
    }

    void deregisterOperation(Address caller, long callId) {
        if (!liveOperations.get(caller).remove(callId)) {
            throw new IllegalStateException("Missing operation for caller=" + caller + " callId=" + callId);
        }
    }
}
