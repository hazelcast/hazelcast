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
import com.hazelcast.client.client.InvocationClientRequest;
import com.hazelcast.cluster.ClusterService;
import com.hazelcast.config.JobTrackerConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.mapreduce.CombinerFactory;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyPredicate;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.ReducerFactory;
import com.hazelcast.mapreduce.TopologyChangedStrategy;
import com.hazelcast.mapreduce.impl.AbstractJobTracker;
import com.hazelcast.mapreduce.impl.HashMapAdapter;
import com.hazelcast.mapreduce.impl.MapReducePortableHook;
import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.mapreduce.impl.operation.KeyValueJobOperation;
import com.hazelcast.mapreduce.impl.operation.StartProcessingJobOperation;
import com.hazelcast.mapreduce.impl.task.TrackableJobFuture;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.security.Permission;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.mapreduce.impl.MapReduceUtil.executeOperation;

/**
 * This class is used to prepare and start a map reduce job emitted by a client
 * on a random node in the cluster (making it the job owner).
 *
 * @param <KeyIn>   type of the input key
 * @param <ValueIn> type of the input value
 */
public class ClientMapReduceRequest<KeyIn, ValueIn> extends InvocationClientRequest {

    protected String name;

    protected String jobId;

    protected Collection keys;

    protected KeyPredicate predicate;

    protected Mapper mapper;

    protected CombinerFactory combinerFactory;

    protected ReducerFactory reducerFactory;

    protected KeyValueSource keyValueSource;

    protected int chunkSize;

    protected TopologyChangedStrategy topologyChangedStrategy;

    public ClientMapReduceRequest() {
    }

    public ClientMapReduceRequest(String name, String jobId, Collection keys, KeyPredicate predicate, Mapper mapper,
                                  CombinerFactory combinerFactory, ReducerFactory reducerFactory, KeyValueSource keyValueSource,
                                  int chunkSize, TopologyChangedStrategy topologyChangedStrategy) {
        this.name = name;
        this.jobId = jobId;
        this.keys = keys;
        this.predicate = predicate;
        this.mapper = mapper;
        this.combinerFactory = combinerFactory;
        this.reducerFactory = reducerFactory;
        this.keyValueSource = keyValueSource;
        this.chunkSize = chunkSize;
        this.topologyChangedStrategy = topologyChangedStrategy;
    }

    @Override
    protected void invoke() {
        try {
            final ClientEndpoint endpoint = getEndpoint();

            MapReduceService mapReduceService = getService();
            NodeEngine nodeEngine = mapReduceService.getNodeEngine();
            AbstractJobTracker jobTracker = (AbstractJobTracker) mapReduceService.createDistributedObject(name);
            TrackableJobFuture jobFuture = new TrackableJobFuture(name, jobId, jobTracker, nodeEngine, null);
            if (jobTracker.registerTrackableJob(jobFuture)) {
                ICompletableFuture<Object> future = startSupervisionTask(jobFuture, mapReduceService, nodeEngine, jobTracker);
                future.andThen(new ExecutionCallback<Object>() {
                    @Override
                    public void onResponse(Object response) {
                        Object clientResponse = response;
                        if (clientResponse instanceof HashMap) {
                            clientResponse = new HashMapAdapter((HashMap) clientResponse);
                        }
                        endpoint.sendResponse(clientResponse, getCallId());
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        Throwable throwable = t;
                        if (throwable instanceof ExecutionException) {
                            throwable = throwable.getCause();
                        }
                        endpoint.sendResponse(throwable, getCallId());
                    }
                });
            }
        } catch (Exception e) {
            throw new IllegalStateException("Could not register map reduce job", e);
        }
    }

    private <T> ICompletableFuture<T> startSupervisionTask(TrackableJobFuture<T> jobFuture, MapReduceService mapReduceService,
                                                           NodeEngine nodeEngine, JobTracker jobTracker) {

        JobTrackerConfig config = ((AbstractJobTracker) jobTracker).getJobTrackerConfig();
        boolean communicateStats = config.isCommunicateStats();
        if (chunkSize == -1) {
            chunkSize = config.getChunkSize();
        }
        if (topologyChangedStrategy == null) {
            topologyChangedStrategy = config.getTopologyChangedStrategy();
        }

        ClusterService cs = nodeEngine.getClusterService();
        Collection<MemberImpl> members = cs.getMemberList();
        for (MemberImpl member : members) {
            Operation operation = new KeyValueJobOperation<KeyIn, ValueIn>(name, jobId, chunkSize, keyValueSource, mapper,
                    combinerFactory, reducerFactory, communicateStats, topologyChangedStrategy);

            executeOperation(operation, member.getAddress(), mapReduceService, nodeEngine);
        }

        // After we prepared all the remote systems we can now start the processing
        for (MemberImpl member : members) {
            Operation operation = new StartProcessingJobOperation<KeyIn>(name, jobId, keys, predicate);
            executeOperation(operation, member.getAddress(), mapReduceService, nodeEngine);
        }
        return jobFuture;
    }

    @Override
    public void write(PortableWriter writer)
            throws IOException {
        super.write(writer);
        ObjectDataOutput out = writer.getRawDataOutput();
        writeData(out);
    }

    @Override
    public void read(PortableReader reader)
            throws IOException {
        super.read(reader);
        ObjectDataInput in = reader.getRawDataInput();
        readData(in);
    }

    @Override
    public String getServiceName() {
        return MapReduceService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return MapReducePortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapReducePortableHook.CLIENT_MAP_REDUCE_REQUEST;
    }

    private void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeUTF(name);
        out.writeUTF(jobId);
        out.writeObject(predicate);
        out.writeObject(mapper);
        out.writeObject(combinerFactory);
        out.writeObject(reducerFactory);
        out.writeObject(keyValueSource);
        out.writeInt(chunkSize);
        out.writeInt(keys == null ? 0 : keys.size());
        if (keys != null) {
            for (Object key : keys) {
                out.writeObject(key);
            }
        }
        out.writeBoolean(topologyChangedStrategy != null);
        if (topologyChangedStrategy != null) {
            out.writeInt(topologyChangedStrategy.ordinal());
        }
    }

    private void readData(ObjectDataInput in)
            throws IOException {
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
        if (in.readBoolean()) {
            topologyChangedStrategy = topologyChangedStrategyByOrdinal(in.readInt());
        }
    }

    private TopologyChangedStrategy topologyChangedStrategyByOrdinal(int ordinal) {
        for (TopologyChangedStrategy temp : TopologyChangedStrategy.values()) {
            if (ordinal == temp.ordinal()) {
                return temp;
            }
        }
        throw new IllegalArgumentException("TopologyChangedStrategy with ordinal " + ordinal + " is unknown");
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }
}
