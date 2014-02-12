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
 * This is a special version of ICompletableFuture to return the assigned job
 * id of the submit operation.
 *
 * @param <V> type of the resulting value
 * @since 3.2
 */
@Beta
public interface JobCompletableFuture<V>
        extends ICompletableFuture<V> {

    /**
     * Returns the unique identifier for this mapreduce job. This jobId is used to identify the same
     * job on all cluster nodes and is unique in the cluster.
     *
     * @return jobId for this job
     */
    String getJobId();

}
