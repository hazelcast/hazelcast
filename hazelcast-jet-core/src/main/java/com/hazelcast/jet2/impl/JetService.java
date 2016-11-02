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
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.util.collection.MPSCQueue;
import com.hazelcast.jet2.JetEngineConfig;
import com.hazelcast.jet2.impl.deployment.DeploymentStore;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.PacketHandler;
import com.hazelcast.util.ConcurrencyUtil;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.instance.OutOfMemoryErrorDispatcher.inspectOutOfMemoryError;
import static com.hazelcast.util.EmptyStatement.ignore;

public class JetService implements ManagedService, RemoteService, PacketHandler {

    public static final String SERVICE_NAME = "hz:impl:jetService";
    private final PacketHandlerThread handler;

    private NodeEngineImpl nodeEngine;
    private final ILogger logger;

    private ConcurrentMap<String, ExecutionContext> executionContexts = new ConcurrentHashMap<>();


    public JetService(NodeEngine nodeEngine) {
        this.nodeEngine = (NodeEngineImpl) nodeEngine;
        this.logger = nodeEngine.getLogger(JetService.class);
        this.handler = new PacketHandlerThread();

    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        handler.start();
    }

    @Override
    public void reset() {

    }

    @Override
    public void shutdown(boolean terminate) {
        for (ExecutionContext executionContext : executionContexts.values()) {
            executionContext.destroy();
        }
        handler.shutdown();
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
        DeploymentStore deploymentStore = ec.getDeploymentStore();
        deploymentStore.destroy();
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
        handler.queue.offer(packet);
    }

    private class PacketHandlerThread extends Thread {

        private final BlockingQueue<Packet> queue = new MPSCQueue<>(this, null);
        private volatile boolean isShutdown;

        @Override
        public void run() {
            try {
                doRun();
            } catch (InterruptedException e) {
                ignore(e);
            } catch (Throwable t) {
                inspectOutOfMemoryError(t);
                logger.severe(t);
            }
        }

        private void doRun() throws InterruptedException {
            while (!isShutdown) {
                Packet packet = queue.take();
                try {
                    Payload payload = (Payload) nodeEngine.toObject(new HeapData(packet.toByteArray()));
                    payload.setPartitionId(packet.getPartitionId());
                    ExecutionContext context = executionContexts.get(payload.getEngineName());
                    assert context != null : "Packet received for unknown execution context";
                    context.handleIncoming(payload);
                } catch (Throwable e) {
                    inspectOutOfMemoryError(e);
                    logger.severe("Error processing packet: " + packet, e);
                }
            }
        }

        private void shutdown() {
            isShutdown = true;
            interrupt();
        }
    }
}
