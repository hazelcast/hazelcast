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
 * This is a special version of ICompletableFuture to return the assigned job ID of the submit operation.
 *
 * @param <V> type of the resulting value
 * @since 3.2
 * @deprecated MapReduce is deprecated and will be removed in 4.0.
 * For map aggregations, you can use {@link com.hazelcast.aggregation.Aggregator} on IMap.
 * For general data processing, it is superseded by <a href="http://jet.hazelcast.org">Hazelcast Jet</a>.
 */
@Deprecated
public interface JobCompletableFuture<V>
        extends ICompletableFuture<V> {

    /**
     * Returns the unique identifier for this mapreduce job. This jobId is used to identify the same
     * job on all cluster nodes and is unique in the cluster.
     *
     * @return jobId, the unique identifier for this mapreduce job
     */
    String getJobId();
}
