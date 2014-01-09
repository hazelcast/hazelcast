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

package com.hazelcast.mapreduce.impl.task;

import com.hazelcast.mapreduce.JobPartitionState;
import com.hazelcast.mapreduce.JobProcessInformation;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.impl.AbstractJobTracker;
import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.mapreduce.impl.MapReduceUtil;
import com.hazelcast.mapreduce.impl.notification.IntermediateChunkNotification;
import com.hazelcast.mapreduce.impl.notification.LastChunkNotification;
import com.hazelcast.mapreduce.impl.notification.MapReduceNotification;
import com.hazelcast.mapreduce.impl.operation.GetResultOperationFactory;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

public class JobSupervisor {

    private final ConcurrentMap<Object, Reducer> reducers = new ConcurrentHashMap<Object, Reducer>();
    private final AtomicReference<DefaultContext> context = new AtomicReference<DefaultContext>();

    private final Address jobOwner;
    private final boolean ownerNode;
    private final AbstractJobTracker jobTracker;
    private final JobTaskConfiguration configuration;
    private final MapReduceService mapReduceService;

    private final JobProcessInformationImpl jobProcessInformation;

    public JobSupervisor(JobTaskConfiguration configuration, AbstractJobTracker jobTracker,
                         boolean ownerNode, MapReduceService mapReduceService) {
        this.jobTracker = jobTracker;
        this.ownerNode = ownerNode;
        this.configuration = configuration;
        this.mapReduceService = mapReduceService;
        this.jobOwner = configuration.getJobOwner();
        this.jobProcessInformation = new JobProcessInformationImpl(
                configuration.getNodeEngine().getPartitionService().getPartitionCount(), this);

        // Preregister reducer task to handle immediate reducing events
        String name = configuration.getName();
        String jobId = configuration.getJobId();
        jobTracker.registerReducerTask(new ReducerTask(name, jobId, this));
    }

    public MapReduceService getMapReduceService() {
        return mapReduceService;
    }

    public JobTracker getJobTracker() {
        return jobTracker;
    }

    public void startTasks(MappingPhase mappingPhase) {
        // Start map-combiner tasks
        jobTracker.registerMapCombineTask(new MapCombineTask(configuration, this, mappingPhase));
    }

    public void onNotification(MapReduceNotification event) {
        if (event instanceof IntermediateChunkNotification) {
            IntermediateChunkNotification icn = (IntermediateChunkNotification) event;
            ReducerTask reducerTask = jobTracker.getReducerTask(icn.getJobId());
            reducerTask.processChunk(icn.getChunk());
        } else if (event instanceof LastChunkNotification) {
            LastChunkNotification lcn = (LastChunkNotification) event;
            ReducerTask reducerTask = jobTracker.getReducerTask(lcn.getJobId());
            System.out.println("Event received: " + lcn.getPartitionId());
            reducerTask.processChunk(lcn.getPartitionId(), lcn.getChunk());
        }
    }

    public Map<Object, Object> getJobResults() {
        Map<Object, Object> result = null;
        if (configuration.getReducerFactory() != null) {
            result = new HashMap<Object, Object>();
            for (Map.Entry<Object, Reducer> entry : reducers.entrySet()) {
                result.put(entry.getKey(), entry.getValue().finalizeReduce());
            }
        } else {
            DefaultContext context = this.context.get();
            result = context.finish();
        }
        return result;
    }

    public <KeyIn, ValueIn, ValueOut> Reducer<KeyIn, ValueIn, ValueOut> getReducerByKey(Object key) {
        Reducer reducer = reducers.get(key);
        if (reducer == null && configuration.getReducerFactory() != null) {
            reducer = configuration.getReducerFactory().newReducer(key);
            reducer = reducers.putIfAbsent(key, reducer);
            reducer.beginReduce(key);
        }
        return reducer;
    }

    public void checkFullyProcessed(JobProcessInformation processInformation) {
        if (isOwnerNode()) {
            JobPartitionState[] partitionStates = processInformation.getPartitionStates();
            for (JobPartitionState partitionState : partitionStates) {
                if (partitionState == null
                        || partitionState.getState() != JobPartitionState.State.PROCESSED) {
                    return;
                }
            }
            System.out.println("Finished: " + MapReduceUtil.printPartitionStates(partitionStates));
            System.out.println("Requesting results...");

            String name = configuration.getName();
            String jobId = configuration.getJobId();
            NodeEngine nodeEngine = configuration.getNodeEngine();
            List<Map> results = MapReduceUtil.executeOperation(new GetResultOperationFactory(name, jobId),
                    mapReduceService, nodeEngine, true);

            boolean reducedResult = configuration.getReducerFactory() != null;

            if (results != null) {
                Map<Object, Object> mergedResults = new HashMap<Object, Object>();
                for (Map<?, ?> map : results) {
                    for (Map.Entry entry : map.entrySet()) {
                        if (reducedResult) {
                            mergedResults.put(entry.getKey(), entry.getValue());

                        } else {
                            List<Object> list = (List) mergedResults.get(entry.getKey());
                            if (list == null) {
                                list = new ArrayList<Object>();
                                mergedResults.put(entry.getKey(), list);
                            }
                            for (Object value : (List) entry.getValue()) {
                                list.add(value);
                            }
                        }
                    }

                    // Set the result and finish the request
                    // TODO Feature Collator
                    TrackableJobFuture future = jobTracker.getTrackableJob(jobId);
                    future.setResult(mergedResults);
                }
            }
        }
    }

    public <K, V> DefaultContext<K, V> createContext(MapCombineTask mapCombineTask) {
        DefaultContext<K, V> context = new DefaultContext<K, V>(
                configuration.getCombinerFactory(), mapCombineTask);

        if (this.context.compareAndSet(null, context)) {
            return context;
        }
        return this.context.get();
    }

    public JobProcessInformationImpl getJobProcessInformation() {
        return jobProcessInformation;
    }

    public Address getJobOwner() {
        return jobOwner;
    }

    public boolean isOwnerNode() {
        return ownerNode;
    }

    public JobTaskConfiguration getConfiguration() {
        return configuration;
    }

}
