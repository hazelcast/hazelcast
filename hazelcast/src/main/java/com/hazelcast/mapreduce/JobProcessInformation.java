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

/**
 * This interface holds basic information about a running map reduce job, such as the
 * state of the different partitions and the number of currently processed
 * records.<br/>
 * The number of processed records is not a real time value, it is updated
 * on a regular basis (after 1000 processed elements per node).
 *
 * @since 3.2
 * @deprecated MapReduce is deprecated and will be removed in 4.0.
 * For map aggregations, you can use {@link com.hazelcast.aggregation.Aggregator} on IMap.
 * For general data processing, it is superseded by <a href="http://jet.hazelcast.org">Hazelcast Jet</a>.
 */
@Deprecated
public interface JobProcessInformation {

    /**
     * Returns an array of {@link com.hazelcast.mapreduce.JobPartitionState}s holding
     * information about the processing state ({@link com.hazelcast.mapreduce.JobPartitionState.State})
     * and the processing owner of this partition.<br/>
     * The index of the {@link com.hazelcast.mapreduce.JobPartitionState} inside of the
     * array is the number of the processed partition if the {@link com.hazelcast.mapreduce.KeyValueSource}
     * is {@link com.hazelcast.mapreduce.PartitionIdAware} or a randomly assigned ID for
     * the different members of the cluster.
     *
     * @return partition state array with actual state information
     */
    JobPartitionState[] getPartitionStates();

    /**
     * Returns the number of processed records.<br/>
     * The number of processed records is not a real time value, it is updated
     * on a regular basis (after 1000 processed elements per node).
     *
     * @return number of processed records
     */
    int getProcessedRecords();

}
