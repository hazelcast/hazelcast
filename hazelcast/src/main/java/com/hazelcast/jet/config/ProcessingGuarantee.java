/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.config;

/**
 * Defines what message processing guarantees are given under failure
 * conditions. Specifically, if a member of the cluster leaves the cluster
 * during job execution and the job is restarted automatically it defines
 * the semantics of at which point in the stream the job is resumed from.
 * <p>
 * When {@link #AT_LEAST_ONCE} or {@link #EXACTLY_ONCE} is set, distributed
 * snapshotting will be enabled for the job. The distributed snapshot algorithm
 * works by sending <i>barriers</i> down the stream which upon receiving causes
 * the processors to save their state as a snapshot. Snapshots are saved
 * in memory and replicated across the cluster.
 * <p>
 * Since a processor can have multiple inputs, it must wait until the barrier is
 * received from all inputs before taking a snapshot. The difference between
 * {@link #AT_LEAST_ONCE} and {@link #EXACTLY_ONCE} is that in
 * {@link #AT_LEAST_ONCE} mode the processor can continue to process items
 * from inputs which have already received the barrier. This will result
 * in lower latency and higher throughput overall, with the caveat that
 * some items may be processed twice after a restart.
 *
 * @since Jet 3.0
 */
public enum ProcessingGuarantee {

    /**
     * No processing guarantees are given and no snapshots are taken during
     * job execution. When a job is restarted automatically it will be as
     * if the job is starting from scratch which can cause items to be lost
     * or duplicated.
     * <p>
     * This option provides the overall best throughput and latency and no
     * storage overheads from snapshotting. However, it doesn't provide any
     * correctness guarantees under failure.
     */
    NONE,

    /**
     * Enables <i>at-least-once</i> processing semantics. When a job is restarted
     * it will be resumed from the latest available snapshot. Items which have been
     * processed before the snapshot might be processed again after the job is resumed.
     * <p>
     * This option requires in-memory snapshotting which will cause additional storage
     * requirements and overhead compared to {@link #NONE}. However it provides better
     * latency than {@link #EXACTLY_ONCE} with weaker guarantees.
     */
    AT_LEAST_ONCE,

    /**
     * Enables <i>exactly-once</i> processing semantics. When a job is restarted
     * it will be resumed from the latest available snapshot. Items which have been
     * processed before the snapshot are guaranteed not to be processed again after
     * the job is resumed.
     * <p>
     * This option requires in-memory snapshotting which will cause additional storage
     * requirements and overhead compared to {@link #NONE}. It provides the strongest
     * correctness guarantee. However latency might increase due to the aligning of
     * barriers which are required in this processing mode.
     */
    EXACTLY_ONCE
}
