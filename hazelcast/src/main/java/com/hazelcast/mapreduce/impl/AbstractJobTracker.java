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

package com.hazelcast.mapreduce.impl;

import com.hazelcast.config.JobTrackerConfig;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.impl.task.MapCombineTask;
import com.hazelcast.mapreduce.impl.task.ReducerTask;
import com.hazelcast.spi.NodeEngine;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

public abstract class AbstractJobTracker implements JobTracker {

    protected final ConcurrentMap<String, TrackableJob> trackableJobs = new ConcurrentHashMap<String, TrackableJob>();
    protected final ConcurrentMap<String, ReducerTask> reducerTasks = new ConcurrentHashMap<String, ReducerTask>();
    protected final ConcurrentMap<String, MapCombineTask> mapCombineTasks = new ConcurrentHashMap<String, MapCombineTask>();
    protected final NodeEngine nodeEngine;
    protected final ExecutorService executorService;
    protected final JobTrackerConfig jobTrackerConfig;
    protected final String name;

    AbstractJobTracker(String name, JobTrackerConfig jobTrackerConfig, NodeEngine nodeEngine) {
        this.name = name;
        this.nodeEngine = nodeEngine;
        this.jobTrackerConfig = jobTrackerConfig;
        this.executorService = nodeEngine.getExecutionService().getExecutor(name);
    }

    @Override
    public void destroy() {
    }

    @Override
    public Object getId() {
        return getName();
    }

    @Override
    public String getPartitionKey() {
        return getName();
    }

    @Override
    public final String getName() {
        return name;
    }

    @Override
    public final String getServiceName() {
        return MapReduceService.SERVICE_NAME;
    }

    public <V> boolean registerTrackableJob(TrackableJob<V> trackableJob) {
        return trackableJobs.putIfAbsent(trackableJob.getJobId(), trackableJob) == trackableJob;
    }

    public <V> boolean unregisterTrackableJob(TrackableJob<V> trackableJob) {
        return trackableJobs.remove(trackableJob) != null;
    }

    public <V> TrackableJob<V> getTrackableJob(String jobId) {
        return trackableJobs.get(jobId);
    }

    public void registerReducerTask(ReducerTask reducerTask) {
        reducerTasks.put(reducerTask.getJobId(), reducerTask);
    }

    public void registerMapCombineTask(MapCombineTask mapCombineTask) {
        mapCombineTasks.put(mapCombineTask.getJobId(), mapCombineTask);
    }

}
