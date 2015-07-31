/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.task.mapreduce;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.task.AbstractMessageTask;
import com.hazelcast.cluster.ClusterService;
import com.hazelcast.config.JobTrackerConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.Member;
import com.hazelcast.instance.Node;
import com.hazelcast.mapreduce.CombinerFactory;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyPredicate;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.ReducerFactory;
import com.hazelcast.mapreduce.TopologyChangedStrategy;
import com.hazelcast.mapreduce.impl.AbstractJobTracker;
import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.mapreduce.impl.operation.KeyValueJobOperation;
import com.hazelcast.mapreduce.impl.operation.StartProcessingJobOperation;
import com.hazelcast.mapreduce.impl.task.TrackableJobFuture;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;

import java.security.Permission;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.mapreduce.impl.MapReduceUtil.executeOperation;

public abstract class AbstractMapReduceTask<Parameters>
        extends AbstractMessageTask<Parameters>
        implements ExecutionCallback {

    public AbstractMapReduceTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        MapReduceService mapReduceService = getService(MapReduceService.SERVICE_NAME);
        NodeEngine nodeEngine = mapReduceService.getNodeEngine();
        final String objectName = getDistributedObjectName();
        AbstractJobTracker jobTracker = (AbstractJobTracker) mapReduceService.createDistributedObject(objectName);
        TrackableJobFuture jobFuture = new TrackableJobFuture(objectName, getJobId(), jobTracker, nodeEngine, null);
        if (jobTracker.registerTrackableJob(jobFuture)) {
            startSupervisionTask(jobTracker);
            jobFuture.andThen(this);
        }
    }

    protected abstract String getJobId();

    protected abstract int getChunkSize();

    protected abstract String getTopologyChangedStrategy();

    protected abstract KeyValueSource getKeyValueSource();

    protected abstract Mapper getMapper();

    protected abstract CombinerFactory getCombinerFactory();

    protected abstract ReducerFactory getReducerFactory();

    protected abstract Collection getKeys();

    protected abstract KeyPredicate getPredicate();

    private void startSupervisionTask(JobTracker jobTracker) {
        MapReduceService mapReduceService = getService(MapReduceService.SERVICE_NAME);

        JobTrackerConfig config = ((AbstractJobTracker) jobTracker).getJobTrackerConfig();
        boolean communicateStats = config.isCommunicateStats();
        int chunkSize = getChunkSize();
        if (chunkSize == -1) {
            chunkSize = config.getChunkSize();
        }
        String topologyChangedStrategyStr = getTopologyChangedStrategy();
        TopologyChangedStrategy topologyChangedStrategy;
        if (topologyChangedStrategyStr == null) {
            topologyChangedStrategy = config.getTopologyChangedStrategy();
        } else {
            topologyChangedStrategy = TopologyChangedStrategy.valueOf(topologyChangedStrategyStr.toUpperCase(Locale.ENGLISH));
        }

        ClusterService cs = nodeEngine.getClusterService();
        Collection<Member> members = cs.getMembers();

        String name = getDistributedObjectName();
        String jobId = getJobId();
        KeyValueSource keyValueSource = getKeyValueSource();
        Mapper mapper = getMapper();
        CombinerFactory combinerFactory = getCombinerFactory();
        ReducerFactory reducerFactory = getReducerFactory();
        Collection keys = getKeys();

        Collection<Object> keyObjects = null;
        if (keys != null) {
            keyObjects = new ArrayList<Object>(keys.size());
            for (Object key : keys) {
                keyObjects.add(serializationService.toObject(key));
            }
        }

        KeyPredicate predicate = getPredicate();

        for (Member member : members) {
            Operation operation = new KeyValueJobOperation(name, jobId, chunkSize, keyValueSource, mapper, combinerFactory,
                    reducerFactory, communicateStats, topologyChangedStrategy);

            executeOperation(operation, member.getAddress(), mapReduceService, nodeEngine);
        }

        // After we prepared all the remote systems we can now start the processing
        for (Member member : members) {
            Operation operation = new StartProcessingJobOperation(name, jobId, keyObjects, predicate);
            executeOperation(operation, member.getAddress(), mapReduceService, nodeEngine);
        }
    }

    @Override
    public void onResponse(Object response) {
        Map<Object, Object> m = (Map<Object, Object>) response;
        Map<Data, Data> hashMap = new HashMap<Data, Data>();
        for (Map.Entry<Object, Object> entry : m.entrySet()) {
            Data key = serializationService.toData(entry.getKey());
            Data value = serializationService.toData(entry.getValue());
            hashMap.put(key, value);
        }
        sendResponse(hashMap.entrySet());
    }

    @Override
    public void onFailure(Throwable t) {
        Throwable throwable = t;
        if (throwable instanceof ExecutionException) {
            throwable = throwable.getCause();
        }
        sendClientMessage(throwable);
    }

    @Override
    public String getServiceName() {
        return MapReduceService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getMethodName() {
        return null;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }
}
