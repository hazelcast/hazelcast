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

package com.hazelcast.mapreduce;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.spi.annotation.Beta;

/**
 * This interface describes a trackable job. In the default implementation
 * the internal assigned operation processor is a TrackableJob so the process
 * can be watched on while processing.<br/>
 * To request the TrackableJob of a map reduce job use the following code snippet
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
 */
@Beta
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
     * Returns the unique job id of the tracked job
     *
     * @return job id of the tracked job
     */
    String getJobId();

    /**
     * Returns the {@link com.hazelcast.core.ICompletableFuture} to add callbacks
     * or wait for the resulting value of the tracked job
     *
     * @return ICompletableFuture of the tracked job
     */
    ICompletableFuture<V> getCompletableFuture();

    /**
     * Returns an instance of {@link com.hazelcast.mapreduce.JobProcessInformation} to find out the state and
     * statistics of a running task or null if the underlying job id is not available because job is already
     * finished or not yet submitted.<br/>
     * It even returns null if not requested on the job issuing cluster member or client since those values are
     * not distributed to all clusters for traffic reasons.
     *
     * @return instance of the jobs process information or null if job id is not available
     */
    JobProcessInformation getJobProcessInformation();

}
