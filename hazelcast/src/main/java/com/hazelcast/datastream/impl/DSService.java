/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.datastream.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.DataStreamConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.datastream.impl.operations.AddListenerOperation;
import com.hazelcast.internal.codeneneration.Compiler;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.function.Consumer;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;

public class DSService implements ManagedService, RemoteService, Consumer<Packet> {
    public static final String SERVICE_NAME = "hz:impl:service";
    private final ConcurrentMap<String, DSContainer> containers = new ConcurrentHashMap<String, DSContainer>();
    private final Compiler compiler = new Compiler("datastream-src");
    //  private final OperationService operationService;
    private final ConcurrentHashMap<String, DSPartitionListeners> subscriptions = new ConcurrentHashMap<>();
    private int bytesInFlight;
    private final ConcurrentMap<String, AtomicLong> bytesInFlightMap = new ConcurrentHashMap<>();

    private final ConstructorFunction<String, DSContainer> containerConstructorFunction =
            new ConstructorFunction<String, DSContainer>() {
                public DSContainer createNew(String key) {
                    Config config = nodeEngine.getConfig();
                    DataStreamConfig dataStreamConfig = config.findDataStreamConfig(key);
                    return new DSContainer(dataStreamConfig, DSService.this, nodeEngine, compiler);
                }
            };

    private NodeEngineImpl nodeEngine;

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = (NodeEngineImpl) nodeEngine;
        this.bytesInFlight = 1024 * 1024 * 10;
    }

    @Override
    public void reset() {
    }

    @Override
    public void shutdown(boolean terminate) {
    }

    public DSContainer getDataStreamContainer(String name) {
        return getOrPutIfAbsent(containers, name, containerConstructorFunction);
    }

    public DSContainer getDataStreamContainer(String name, final DataStreamConfig config) {
        return getOrPutIfAbsent(containers, name, key -> new DSContainer(config, this, nodeEngine, compiler));
    }

    @Override
    public DistributedObject createDistributedObject(String name) {
        return new DSProxy(name, nodeEngine, this);
    }

    @Override
    public void destroyDistributedObject(String objectName) {
    }

    @Override
    public void accept(Packet packet) {
        // todo: it can be a registration
        // todo: it can be event data.
    }

    public void startListening(String streamName, DataInputStreamImpl subscriber, List<Integer> partitionIds, List<Long> offsets) {
        List<InternalCompletableFuture> futures = new LinkedList<>();
        OperationService operationService = nodeEngine.getOperationService();

        if (partitionIds == null) {
            for (int k = 0; k < nodeEngine.getPartitionService().getPartitionCount(); k++) {
                partitionIds.add(k);
            }
        }

        for (int k = 0; k < partitionIds.size(); k++) {
            int partitionId = partitionIds.get(k);
            long offset = offsets == null ? -1 : offsets.get(k);
            Operation operation = new AddListenerOperation(streamName, offset, subscriber)
                    .setPartitionId(partitionId);
            futures.add(operationService.invokeOnPartition(operation));
        }

        for (InternalCompletableFuture future : futures) {
            future.join();
        }
    }

    public DSPartitionListeners getOrCreatePartitionListeners(String name, int partitionId, DSPartition partition) {
        String id = name + "_" + partitionId;
        DSPartitionListeners listeners = subscriptions.get(id);
        if (listeners == null) {
            listeners = new DSPartitionListeners(this, name, partition, (InternalSerializationService) nodeEngine.getSerializationService());
            DSPartitionListeners old = subscriptions.putIfAbsent(id, listeners);
            if (old != null) {
                listeners = old;
            }
        }
        return listeners;
    }

    public AtomicLong getBytesInFlight(String uuid) {
        AtomicLong b = bytesInFlightMap.get(uuid);
        if (b == null) {
            b = new AtomicLong(bytesInFlight);
            AtomicLong found = bytesInFlightMap.putIfAbsent(uuid, b);
            b = found == null ? b : found;
        }
        return b;
    }
}
