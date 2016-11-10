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
import com.hazelcast.logging.ILogger;
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
    public static final Charset CHARSET = Charset.forName("UTF-8");

    private NodeEngineImpl nodeEngine;
    private final ILogger logger;

    private ConcurrentMap<String, EngineContext> engineContexts = new ConcurrentHashMap<>();
    private ConcurrentMap<Address, Map<Long, Boolean>> liveOperations = new ConcurrentHashMap<>();

    public JetService(NodeEngine nodeEngine) {
        this.nodeEngine = (NodeEngineImpl) nodeEngine;
        this.logger = nodeEngine.getLogger(JetService.class);
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

    @Override
    public void handle(Packet packet) throws Exception {
        int offset = 0;
        byte[] bytes = packet.toByteArray();
        int length = Bits.readIntB(bytes, offset);
        offset += Bits.INT_SIZE_IN_BYTES;
        String engineName = new String(bytes, offset, length, CHARSET);
        offset += length;
        long executionId = Bits.readLongB(bytes, offset);
        offset += Bits.LONG_SIZE_IN_BYTES;
        int vertexId = Bits.readIntB(bytes, offset);
        offset += Bits.INT_SIZE_IN_BYTES;
        int ordinal = Bits.readIntB(bytes, offset);
        offset += Bits.INT_SIZE_IN_BYTES;

        engineContexts
                .get(engineName)
                .getExecutionContext(executionId)
                .handlePacket(vertexId, ordinal, packet.getPartitionId(), bytes, offset);
    }

    @Override
    public void populate(LiveOperations liveOperations) {
        this.liveOperations.entrySet().forEach(entry -> entry.getValue().keySet().stream().forEach(callId ->
                liveOperations.add(entry.getKey(), callId)));
    }
}
