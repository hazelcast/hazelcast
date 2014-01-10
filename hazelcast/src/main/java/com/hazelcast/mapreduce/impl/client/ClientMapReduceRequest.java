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
import com.hazelcast.cluster.ClusterService;
import com.hazelcast.config.JobTrackerConfig;
import com.hazelcast.core.CompletableFuture;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.mapreduce.CombinerFactory;
import com.hazelcast.mapreduce.KeyPredicate;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.ReducerFactory;
import com.hazelcast.mapreduce.impl.AbstractJobTracker;
import com.hazelcast.mapreduce.impl.MapReduceDataSerializerHook;
import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.mapreduce.impl.operation.KeyValueJobOperation;
import com.hazelcast.mapreduce.impl.operation.StartProcessingJobOperation;
import com.hazelcast.mapreduce.impl.task.TrackableJobFuture;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import static com.hazelcast.mapreduce.impl.MapReduceUtil.executeOperation;

public class ClientMapReduceRequest<KeyIn, ValueIn>
        extends InvocationClientRequest
        implements IdentifiedDataSerializable {

    protected String name;

    protected String jobId;

    protected Collection keys;

    protected KeyPredicate predicate;

    protected Mapper mapper;

    protected CombinerFactory combinerFactory;

    protected ReducerFactory reducerFactory;

    protected KeyValueSource keyValueSource;

    protected int chunkSize;

    public ClientMapReduceRequest() {
    }

    public ClientMapReduceRequest(String name, String jobId, Collection keys, KeyPredicate predicate, Mapper mapper,
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
        try {
            final ClientEndpoint endpoint = getEndpoint();
            final ClientEngine engine = getClientEngine();

            MapReduceService mapReduceService = getService();
            NodeEngine nodeEngine = mapReduceService.getNodeEngine();
            AbstractJobTracker jobTracker = (AbstractJobTracker) mapReduceService.createDistributedObject(name);
            TrackableJobFuture jobFuture = new TrackableJobFuture(name, jobId, jobTracker, nodeEngine, null);
            if (jobTracker.registerTrackableJob(jobFuture)) {
                CompletableFuture<Object> future = startSupervisionTask(jobFuture, mapReduceService, nodeEngine, jobTracker);
                future.andThen(new ExecutionCallback<Object>() {
                    @Override
                    public void onResponse(Object response) {
                        engine.sendResponse(endpoint, response);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        engine.sendResponse(endpoint, t);
                    }
                });
            }
        } catch (Exception e) {
            throw new IllegalStateException("Could not register map reduce job", e);
        }
    }

    private <T> CompletableFuture<T> startSupervisionTask(TrackableJobFuture<T> jobFuture,
                                                          MapReduceService mapReduceService,
                                                          NodeEngine nodeEngine,
                                                          AbstractJobTracker jobTracker) {

        JobTrackerConfig config = jobTracker.getJobTrackerConfig();
        boolean communicateStats = config.isCommunicateStats();
        if (chunkSize == -1) {
            chunkSize = config.getChunkSize();
        }

        ClusterService cs = nodeEngine.getClusterService();
        Collection<MemberImpl> members = cs.getMemberList();
        for (MemberImpl member : members) {
            Operation operation = new KeyValueJobOperation<KeyIn, ValueIn>(name, jobId, chunkSize,
                    keyValueSource, mapper, combinerFactory, reducerFactory, communicateStats);

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
        if (keys != null) {
            for (Object key : keys) {
                out.writeObject(key);
            }
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

}
