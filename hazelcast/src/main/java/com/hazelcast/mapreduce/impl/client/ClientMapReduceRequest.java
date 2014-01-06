/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.mapreduce.impl.client;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.ClientEngine;
import com.hazelcast.client.InvocationClientRequest;
import com.hazelcast.core.CompletableFuture;
import com.hazelcast.mapreduce.*;
import com.hazelcast.mapreduce.impl.MapReduceDataSerializerHook;
import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.mapreduce.impl.MapReduceUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Future;

public class ClientMapReduceRequest
        extends InvocationClientRequest
        implements IdentifiedDataSerializable {

    protected String name;

    protected String jobId;

    protected List keys;

    protected KeyPredicate predicate;

    protected Mapper mapper;

    protected CombinerFactory combinerFactory;

    protected ReducerFactory reducerFactory;

    protected KeyValueSource keyValueSource;

    protected int chunkSize;

    public ClientMapReduceRequest() {
    }

    public ClientMapReduceRequest(String name, String jobId, List keys, KeyPredicate predicate, Mapper mapper,
                                  CombinerFactory combinerFactory, ReducerFactory reducerFactory,
                                  KeyValueSource keyValueSource, int chunkSize) {
        this.name = name;
        this.jobId = jobId;
        this.keys = keys;
        this.predicate = predicate;
        this.mapper = mapper;
        this.combinerFactory = combinerFactory;
        this.reducerFactory = reducerFactory;
        this.keyValueSource = keyValueSource;
        this.chunkSize = chunkSize;
    }

    @Override
    protected void invoke() {
        // MapReduceOperationFactory factory = buildFactoryAdapter();

        ClientEndpoint endpoint = getEndpoint();
        ClientEngine engine = getClientEngine();
        SerializationService ss = engine.getSerializationService();
        PartitionService ps = engine.getPartitionService();

        Map<Integer, CompletableFuture> futures = new HashMap<Integer, CompletableFuture>();
        Map<Integer, List> mappedKeys = MapReduceUtil.mapKeysToPartition(ps, keys);
        for (Map.Entry<Integer, List> entry : mappedKeys.entrySet()) {
            Operation op = null; // factory.createOperation(entry.getKey(), entry.getValue());
            InvocationBuilder builder = buildInvocationBuilder(getServiceName(), op, entry.getKey());
            futures.put(entry.getKey(), builder.invoke());
        }

        Map<Integer, Object> results = new HashMap<Integer, Object>();
        for (Map.Entry<Integer, CompletableFuture> entry : futures.entrySet()) {
            try {
                results.put(entry.getKey(), toObject(ss, entry.getValue().get()));
            } catch (Throwable t) {
                results.put(entry.getKey(), t);
            }
        }

        List<Integer> failedPartitions = new LinkedList<Integer>();
        for (Map.Entry<Integer, Object> entry : results.entrySet()) {
            if (entry.getValue() instanceof Throwable) {
                failedPartitions.add(entry.getKey());
            }
        }

        for (Integer partitionId : failedPartitions) {
            List keys = mappedKeys.get(partitionId);
            Operation operation = null; // factory.createOperation(partitionId, keys);
            InvocationBuilder builder = buildInvocationBuilder(getServiceName(), operation, partitionId);
            results.put(partitionId, builder.invoke());
        }

        for (Integer failedPartition : failedPartitions) {
            try {
                Future<?> future = (Future<?>) results.get(failedPartition);
                Object result = future.get();
                results.put(failedPartition, result);
            } catch (Throwable t) {
                results.put(failedPartition, t);
            }
        }

        engine.sendResponse(endpoint, results);
    }

    @Override
    public String getServiceName() {
        return MapReduceService.SERVICE_NAME;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(jobId);
        out.writeObject(predicate);
        out.writeObject(mapper);
        out.writeObject(combinerFactory);
        out.writeObject(reducerFactory);
        out.writeObject(keyValueSource);
        out.writeInt(chunkSize);
        out.writeInt(keys == null ? 0 : keys.size());
        for (Object key : keys) {
            out.writeObject(key);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        jobId = in.readUTF();
        predicate = in.readObject();
        mapper = in.readObject();
        combinerFactory = in.readObject();
        reducerFactory = in.readObject();
        keyValueSource = in.readObject();
        chunkSize = in.readInt();
        int size = in.readInt();
        keys = new ArrayList(size);
        for (int i = 0; i < size; i++) {
            keys.add(in.readObject());
        }
    }

    @Override
    public int getFactoryId() {
        return MapReduceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapReduceDataSerializerHook.CLIENT_MAP_REDUCE_REQUEST;
    }

    private InvocationBuilder buildInvocationBuilder(String serviceName, Operation operation, int partitionId) {
        return createInvocationBuilder(serviceName, operation, partitionId).setExecutorName(MapReduceUtil.buildExecutorName(name));
    }

    private Object toObject(SerializationService ss, Object value) {
        if (value instanceof Data) {
            return ss.toObject((Data) value);
        }
        return value;
    }

}
