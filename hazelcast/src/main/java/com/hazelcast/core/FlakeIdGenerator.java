/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.core;

import com.hazelcast.internal.cluster.ClusterService;

/**
 * Creates cluster-wide unique ID generator. Generated IDs are {@code long} primitive values
 * and are k-ordered (roughly ordered). IDs are in the range from {@code Long.MIN_VALUE} to
 * {@code Long.MAX_VALUE}.
 * <p>
 * The IDs contain timestamp component and a node ID, which is assigned when the member
 * joins the cluster. This allows the IDs to be ordered and unique without any coordination between
 * members, which makes the generator safe even in split-brain scenario (for caveats,
 * {@link ClusterService#getMemberListJoinVersion() see here}).
 * <p>
 * Timestamp component is in milliseconds since 1.1.2017, 0:00 GMT, and has 42 bits. This caps
 * the useful lifespan of the generator to little less than 140 years. The sequence component is 6 bits.
 * If more than 64 IDs are requested in single millisecond, IDs will gracefully overflow to next
 * millisecond and uniqueness is guaranteed in this case.
 *
 * <h4>Node ID overflow</h4>
 * Node ID component of the ID has 16 bits. Members with member list join version higher than
 * 2^16 won't be able to generate IDs. The clients will be able to generate IDs as long as there is at
 * least one member with join version smaller than 2^16. The remedy is to restart the cluster:
 * nodeId will be assigned from zero again. Uniqueness after the restart will be preserved thanks to
 * the timestamp component.
 *
 * <h4>Performance</h4>
 * Operation on member is always local. On client, the {@link #newId()} method goes to a random
 * member and gets a batch of IDs, which will then be returned locally for limited time. The pre-fetch
 * size and the validity time are configured per-client.
 * The {@link #newIdBatch(int)} always goes to a member.
 */
public interface FlakeIdGenerator extends DistributedObject {

    /**
     * Generates and returns a batch of cluster-wide unique IDs. IDs are generated in one network
     * roundtrip to member (or locally, if member instance is used).
     *
     * @param batchSize Requested number of IDs.
     * @return Iterable batch of unique IDs
     *
     * @throws com.hazelcast.concurrent.flakeidgen.FlakeIdNodeIdOverflowException <br>
     *  - on client: if node ID for all members in the cluster overflowed<br>
     *  - on member: if node ID on local member overflowed<br>
     * See "Node ID overflow" in {@link FlakeIdGenerator class documentation} for more details.
     */
    IdBatch newIdBatch(int batchSize);

    /**
     * Generates and returns a cluster-wide unique ID.
     * <p>
     * The call is always local on member. A batch of IDs is pre-fetched on the client and then used for
     * preconfigured time locally. If you need multiple IDs at once, use {@link #newIdBatch(int)}.
     * <p>
     * <b>Note:</b> Values returned from this method can be not strictly ordered.
     *
     * @return new cluster-wide unique ID
     *
     * @throws com.hazelcast.concurrent.flakeidgen.FlakeIdNodeIdOverflowException <br>
     *  - on client: if node ID for all members in the cluster overflowed<br>
     *  - on member: if node ID on local member overflowed<br>
     * See "Node ID overflow" in {@link FlakeIdGenerator class documentation} for more details.
     */
    long newId();
}
