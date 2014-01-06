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

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

public class JobSupervisor {

    private final ConcurrentMap<Object, Reducer> reducers = new ConcurrentHashMap<Object, Reducer>();

    private final AbstractJobTracker jobTracker;
    private final JobTaskConfiguration configuration;
    private final MapReduceService mapReduceService;

    private final Queue reducerQueue = new ConcurrentLinkedQueue();

    public JobSupervisor(JobTaskConfiguration configuration, AbstractJobTracker jobTracker,
                         MapReduceService mapReduceService) {
        this.jobTracker = jobTracker;
        this.configuration = configuration;
        this.mapReduceService = mapReduceService;
    }

    public MapReduceService getMapReduceService() {
        return mapReduceService;
    }

    public JobTracker getJobTracker() {
        return jobTracker;
    }

    public void startTasks(MappingPhase mappingPhase) {
        int chunkSize = configuration.getChunkSize();
        String name = configuration.getName();
        String jobId = configuration.getJobId();
        Mapper mapper = configuration.getMapper();
        KeyValueSource keyValueSource = configuration.getKeyValueSource();
        CombinerFactory combinerFactory = configuration.getCombinerFactory();

        // Start reducer task
        jobTracker.registerReducerTask(new ReducerTask(name, jobId, this, reducerQueue));

        // Start map-combiner tasks
        jobTracker.registerMapCombineTask(new MapCombineTask(configuration, mapReduceService, mappingPhase));

    }

    public void onNotification(MapReduceNotification notification) {
        if (notification instanceof IntermediateChunkNotification) {
            reducerQueue.offer(((IntermediateChunkNotification) notification).getChunk());
        } else if (notification instanceof LastChunkNotification) {
            reducerQueue.offer(((LastChunkNotification) notification).getChunk());
        }
    }

    private <KeyIn, ValueIn, ValueOut> Reducer<KeyIn, ValueIn, ValueOut> getReducerByKey(Object key) {
        Reducer reducer = reducers.get(key);
        if (reducer == null) {
            reducer = configuration.getReducerFactory().newReducer(key);
            reducer = reducers.putIfAbsent(key, reducer);
        }
        return reducer;
    }

}
