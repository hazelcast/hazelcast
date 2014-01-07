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

import com.hazelcast.mapreduce.*;
import com.hazelcast.mapreduce.impl.AbstractJobTracker;
import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.mapreduce.impl.notification.IntermediateChunkNotification;
import com.hazelcast.mapreduce.impl.notification.LastChunkNotification;
import com.hazelcast.mapreduce.impl.notification.MapReduceNotification;
import com.hazelcast.nio.Address;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

public class JobSupervisor {

    private final ConcurrentMap<Object, Reducer> reducers = new ConcurrentHashMap<Object, Reducer>();

    private final Address jobOwner;
    private final AbstractJobTracker jobTracker;
    private final JobTaskConfiguration configuration;
    private final MapReduceService mapReduceService;

    private final JobProcessInformationImpl jobProcessInformation;

    public JobSupervisor(JobTaskConfiguration configuration, AbstractJobTracker jobTracker,
                         MapReduceService mapReduceService) {
        this.jobTracker = jobTracker;
        this.configuration = configuration;
        this.mapReduceService = mapReduceService;
        this.jobOwner = configuration.getJobOwner();
        this.jobProcessInformation = new JobProcessInformationImpl(
                configuration.getNodeEngine().getPartitionService().getPartitionCount());
    }

    public MapReduceService getMapReduceService() {
        return mapReduceService;
    }

    public JobTracker getJobTracker() {
        return jobTracker;
    }

    public void startTasks(MappingPhase mappingPhase) {
        String name = configuration.getName();
        String jobId = configuration.getJobId();

        // Start reducer task
        jobTracker.registerReducerTask(new ReducerTask(name, jobId, this, new ConcurrentLinkedQueue()));

        // Start map-combiner tasks
        jobTracker.registerMapCombineTask(new MapCombineTask(configuration, this, mappingPhase));
    }

    public void onNotification(MapReduceNotification notification) {
        if (notification instanceof IntermediateChunkNotification) {
            IntermediateChunkNotification icn = (IntermediateChunkNotification) notification;
            ReducerTask reducerTask = jobTracker.getReducerTask(icn.getJobId());
            reducerTask.processChunk(icn.getChunk());
        } else if (notification instanceof LastChunkNotification) {
            LastChunkNotification lcn = (LastChunkNotification) notification;
            ReducerTask reducerTask = jobTracker.getReducerTask(lcn.getJobId());
            reducerTask.processChunk(lcn.getChunk());
        }
    }

    public <KeyIn, ValueIn, ValueOut> Reducer<KeyIn, ValueIn, ValueOut> getReducerByKey(Object key) {
        Reducer reducer = reducers.get(key);
        if (reducer == null) {
            reducer = configuration.getReducerFactory().newReducer(key);
            reducer = reducers.putIfAbsent(key, reducer);
            reducer.beginReduce(key);
        }
        return reducer;
    }

    public <Key> Map<Key, Reducer> getReducers() {
        return (Map<Key, Reducer>) reducers;
    }

    public JobProcessInformationImpl getJobProcessInformation() {
        return jobProcessInformation;
    }

    public Address getJobOwner() {
        return jobOwner;
    }

}
