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
import com.hazelcast.client.impl.protocol.parameters.GenericResultParameters;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.cluster.ClusterService;
import com.hazelcast.config.JobTrackerConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.mapreduce.CombinerFactory;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyPredicate;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.ReducerFactory;
import com.hazelcast.mapreduce.TopologyChangedStrategy;
import com.hazelcast.mapreduce.impl.AbstractJobTracker;
import com.hazelcast.mapreduce.impl.HashMapAdapter;
import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.mapreduce.impl.operation.KeyValueJobOperation;
import com.hazelcast.mapreduce.impl.operation.StartProcessingJobOperation;
import com.hazelcast.mapreduce.impl.task.TrackableJobFuture;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;

import java.security.Permission;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.mapreduce.impl.MapReduceUtil.executeOperation;

public abstract class AbstractMapReduceTask<Parameters> extends AbstractCallableMessageTask<Parameters>
        implements ExecutionCallback {

    public AbstractMapReduceTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected ClientMessage call() {
        MapReduceService mapReduceService = getService(MapReduceService.SERVICE_NAME);
        NodeEngine nodeEngine = mapReduceService.getNodeEngine();
        final String objectName = getDistributedObjectName();
        AbstractJobTracker jobTracker = (AbstractJobTracker)
                mapReduceService.createDistributedObject(objectName);
        TrackableJobFuture jobFuture = new TrackableJobFuture(objectName, getJobId(), jobTracker, nodeEngine, null);
        if (jobTracker.registerTrackableJob(jobFuture)) {
            startSupervisionTask(jobTracker);
            jobFuture.andThen(this);
        }

        return null;
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
        TopologyChangedStrategy topologyChangedStrategy = null;
        if (topologyChangedStrategyStr.equals("null")) {
            topologyChangedStrategy = config.getTopologyChangedStrategy();
        } else {
            topologyChangedStrategy =
                    TopologyChangedStrategy.valueOf(topologyChangedStrategyStr.toUpperCase(Locale.ENGLISH));
        }

        ClusterService cs = nodeEngine.getClusterService();
        Collection<MemberImpl> members = cs.getMemberList();

        String name = getDistributedObjectName();
        String jobId = getJobId();
        KeyValueSource keyValueSource = getKeyValueSource();
        Mapper mapper = getMapper();
        CombinerFactory combinerFactory = getCombinerFactory();
        ReducerFactory reducerFactory = getReducerFactory();
        Collection keys = getKeys();
        KeyPredicate predicate = getPredicate();

        for (MemberImpl member : members) {
            Operation operation = new KeyValueJobOperation(name, jobId, chunkSize, keyValueSource, mapper,
                    combinerFactory, reducerFactory, communicateStats, topologyChangedStrategy);

            executeOperation(operation, member.getAddress(), mapReduceService, nodeEngine);
        }

        // After we prepared all the remote systems we can now start the processing
        for (MemberImpl member : members) {
            Operation operation = new StartProcessingJobOperation(name, jobId, keys, predicate);
            executeOperation(operation, member.getAddress(), mapReduceService, nodeEngine);
        }
    }

    @Override
    public void onResponse(Object response) {

        Object clientResponse = response;
        if (clientResponse instanceof HashMap) {
            clientResponse = new HashMapAdapter((HashMap) clientResponse);
        }
        sendClientMessage(GenericResultParameters.encode(serializationService.toData(clientResponse)));
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
