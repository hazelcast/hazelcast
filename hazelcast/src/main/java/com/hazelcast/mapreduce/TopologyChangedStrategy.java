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

import com.hazelcast.spi.annotation.Beta;

/**
 * This enum class is used to define how a map reduce job behaves
 * if the job owner recognizes a topology changed event.<br/>
 * When members are leaving the cluster it might happen to loose
 * processed data chunks that were already send to the reducers
 * on the leaving node.<br/>
 * In addition to that on any topology change there is a redistribution
 * of the member assigned partitions which means that a map job might
 * have a problem to finish it's currently processed partition.<br/>
 * The default behavior is immediately cancelling the running task and
 * throwing an {@link TopologyChangedException} but it is possible
 * to submit the same job configuration again if
 * {@link com.hazelcast.mapreduce.JobTracker#getTrackableJob(String)}
 * returns null for the requested job id.
 *
 * @since 3.2
 */
@Beta
public enum TopologyChangedStrategy {

    /**
     * Default behavior, the currently running job is cancelled
     * immediately on recognizing the topology changed. An
     * {@link TopologyChangedException} is thrown on the job owning
     * node.
     */
    CANCEL_RUNNING_OPERATION,

    /**
     * <b>Attention: This strategy is currently not available but
     * reserved for later usage!</b><br/>
     * Some or all processed data and intermediate results are
     * discarded and the job is automatically restarted.<br/>
     * Depending on the implementation the job might start from
     * an earlier reached safepoint and is not restarted at the
     * beginning.
     */
    DISCARD_AND_RESTART,

    /**
     * <b>Attention: This strategy is currently not available but
     * reserved for later usage!</b><br/>
     * Currently running processes define a safepoint, migrate
     * gracefully and continue their work on new partition owners.<br/>
     * If a member lefts the cluster intermediately reduced data are lost!
     */
    MIGRATE_AND_CONTINUE,

}
