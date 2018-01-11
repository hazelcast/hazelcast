/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.mapreduce;

import com.hazelcast.core.ICompletableFuture;

/**
 * This interface describes a trackable job. In the default implementation,
 * the internal assigned operation processor is a TrackableJob, so the process
 * can be watched while processing.<br/>
 * To request the TrackableJob of a map reduce job, you can use the following code snippet.
 * <pre>
 * JobTracker tracker = hazelcastInstance.getJobTracker(...);
 * Job job = tracker.newJob(...);
 * // ...
 * JobCompletableFuture future = job.submit();
 * String jobId = future.getJobId();
 * TrackableJob trackableJob = tracker.getTrackableJob(jobId);
 * </pre>
 *
 * @param <V> type of the returning value
 * @since 3.2
 * @deprecated MapReduce is deprecated and will be removed in 4.0.
 * For map aggregations, you can use {@link com.hazelcast.aggregation.Aggregator} on IMap.
 * For general data processing, it is superseded by <a href="http://jet.hazelcast.org">Hazelcast Jet</a>.
 */
@Deprecated
public interface TrackableJob<V> {

    /**
     * Returns the assigned {@link JobTracker}
     *
     * @return assigned JobTracker
     */
    JobTracker getJobTracker();

    /**
     * Returns the name of the underlying {@link JobTracker}
     *
     * @return name of the JobTracker
     */
    String getName();

    /**
     * Returns the unique job ID of the tracked job
     *
     * @return job ID of the tracked job
     */
    String getJobId();

    /**
     * Returns the {@link com.hazelcast.core.ICompletableFuture} to add callbacks
     * or wait for the resulting value of the tracked job.
     *
     * @return ICompletableFuture of the tracked job
     */
    ICompletableFuture<V> getCompletableFuture();

    /**
     * Returns an instance of {@link com.hazelcast.mapreduce.JobProcessInformation} to find out the state and
     * statistics of a running task, or returns null if the underlying job ID is not available because the job is already
     * finished or has not yet been submitted.<br/>
     * It returns null if not requested on the job issuing cluster member or client, since those values are
     * not distributed to all clusters for traffic reasons.
     *
     * @return instance of the job process information, or null if job ID is not available
     */
    JobProcessInformation getJobProcessInformation();
}
