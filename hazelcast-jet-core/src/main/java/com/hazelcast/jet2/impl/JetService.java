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
import com.hazelcast.jet2.impl.deployment.DeploymentStore;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.PacketHandler;
import com.hazelcast.util.ConcurrencyUtil;

import java.nio.charset.Charset;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class JetService implements ManagedService, RemoteService, PacketHandler {

    public static final String SERVICE_NAME = "hz:impl:jetService";
    public static final Charset CHARSET = Charset.forName("UTF-8");

    private NodeEngineImpl nodeEngine;
    private final ILogger logger;

    private ConcurrentMap<String, ExecutionContext> executionContexts = new ConcurrentHashMap<>();


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
        for (ExecutionContext executionContext : executionContexts.values()) {
            executionContext.destroy();
        }
    }

    @Override
    public DistributedObject createDistributedObject(String objectName) {
        return new JetEngineImpl(objectName, nodeEngine, this);

    }

    @Override
    public void destroyDistributedObject(String objectName) {
        ExecutionContext ec = executionContexts.remove(objectName);
        if (ec == null) {
            return;
        }
        ec.destroy();
    }

    public void ensureContext(String name, JetEngineConfig config) {
        ConcurrencyUtil.getOrPutSynchronized(executionContexts, name, this,
                (key) -> new ExecutionContext(name, nodeEngine, config));
    }

    public ExecutionContext getExecutionContext(String name) {
        return executionContexts.get(name);
    }


    @Override
    public void handle(Packet packet) throws Exception {
        int offset = 0;
        byte[] bytes = packet.toByteArray();
        int length = Bits.readIntB(bytes, offset);
        offset += Bits.INT_SIZE_IN_BYTES;
        String engineName = new String(bytes, offset, length);
        offset += length;
        ExecutionContext context = executionContexts.get(engineName);
        long executionId = Bits.readLongB(bytes, offset);
        offset += Bits.LONG_SIZE_IN_BYTES;
        int vertexId = Bits.readIntB(bytes, offset);
        offset += Bits.INT_SIZE_IN_BYTES;
        int ordinal = Bits.readIntB(bytes, offset);
        offset += Bits.INT_SIZE_IN_BYTES;
        context.handleIncoming(executionId, vertexId, ordinal, packet.getPartitionId(), bytes, offset);
    }
}
