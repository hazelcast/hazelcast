/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.cluster.memberselector.MemberSelectors;
import com.hazelcast.config.JobTrackerConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.Member;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.ClusterService;
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
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.mapreduce.impl.MapReduceUtil.executeOperation;
import static com.hazelcast.util.StringUtil.LOCALE_INTERNAL;

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

        ClusterService clusterService = nodeEngine.getClusterService();
        if (clusterService.getSize(MemberSelectors.DATA_MEMBER_SELECTOR) == 0) {
            throw new IllegalStateException("Could not register map reduce job since there are no nodes owning a partition");
        }

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
        final MapReduceService mapReduceService = getService(MapReduceService.SERVICE_NAME);

        final JobTrackerConfig config = ((AbstractJobTracker) jobTracker).getJobTrackerConfig();
        final boolean communicateStats = config.isCommunicateStats();
        final int chunkSize = getChunkSizeOrConfigChunkSize(config);
        final TopologyChangedStrategy topologyChangedStrategy = getTopologyChangedStrategyOrConfigTopologyChangedStrategy(config);

        final String name = getDistributedObjectName();
        final String jobId = getJobId();
        final KeyValueSource keyValueSource = getKeyValueSource();
        final Mapper mapper = getMapper();
        final CombinerFactory combinerFactory = getCombinerFactory();
        final ReducerFactory reducerFactory = getReducerFactory();
        final Collection keys = getKeys();

        final Collection<Object> keyObjects = getKeyObjects(keys);

        final KeyPredicate predicate = getPredicate();

        final ClusterService clusterService = nodeEngine.getClusterService();
        for (Member member : clusterService.getMembers(KeyValueJobOperation.MEMBER_SELECTOR)) {
            Operation operation = new KeyValueJobOperation(name, jobId, chunkSize, keyValueSource, mapper, combinerFactory,
                    reducerFactory, communicateStats, topologyChangedStrategy);

            executeOperation(operation, member.getAddress(), mapReduceService, nodeEngine);
        }

        // After we prepared all the remote systems we can now start the processing
        for (Member member : clusterService.getMembers(DATA_MEMBER_SELECTOR)) {
            Operation operation = new StartProcessingJobOperation(name, jobId, keyObjects, predicate);
            executeOperation(operation, member.getAddress(), mapReduceService, nodeEngine);
        }
    }

    private int getChunkSizeOrConfigChunkSize(JobTrackerConfig config) {
        int chunkSize = getChunkSize();
        if (chunkSize == -1) {
            chunkSize = config.getChunkSize();
        }
        return chunkSize;
    }

    private TopologyChangedStrategy getTopologyChangedStrategyOrConfigTopologyChangedStrategy(JobTrackerConfig config) {
        String topologyChangedStrategyStr = getTopologyChangedStrategy();
        TopologyChangedStrategy topologyChangedStrategy;
        if (topologyChangedStrategyStr == null) {
            topologyChangedStrategy = config.getTopologyChangedStrategy();
        } else {
            topologyChangedStrategy = TopologyChangedStrategy.valueOf(topologyChangedStrategyStr.toUpperCase(LOCALE_INTERNAL));
        }
        return topologyChangedStrategy;
    }

    private Collection<Object> getKeyObjects(Collection keys) {
        Collection<Object> keyObjects = null;
        if (keys != null) {
            keyObjects = new ArrayList<Object>(keys.size());
            for (Object key : keys) {
                keyObjects.add(serializationService.toObject(key));
            }
        }
        return keyObjects;
    }

    @Override
    public void onResponse(Object response) {
        Map<Object, Object> m = (Map<Object, Object>) response;
        List<Map.Entry<Data, Data>> entries = new ArrayList<Map.Entry<Data, Data>>();
        for (Map.Entry<Object, Object> entry : m.entrySet()) {
            Data key = serializationService.toData(entry.getKey());
            Data value = serializationService.toData(entry.getValue());
            entries.add(new AbstractMap.SimpleEntry<Data, Data>(key, value));
        }
        sendResponse(entries);
    }

    @Override
    public void onFailure(Throwable t) {
        handleProcessingFailure(t);
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
