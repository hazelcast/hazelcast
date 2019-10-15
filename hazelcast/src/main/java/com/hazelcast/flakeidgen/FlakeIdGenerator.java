/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.flakeidgen;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.cluster.ClusterService;

/**
 * A cluster-wide unique ID generator. Generated IDs are {@code long} primitive values
 * and are k-ordered (roughly ordered). IDs are in the range from {@code 0} to {@code
 * Long.MAX_VALUE}.
 * <p>
 * The IDs contain timestamp component and a node ID component, which is assigned when the member
 * joins the cluster. This allows the IDs to be ordered and unique without any coordination between
 * members, which makes the generator safe even in split-brain scenario (for caveats,
 * {@link ClusterService#getMemberListJoinVersion() see here}).
 * <p>
 * Timestamp component is in milliseconds since 1.1.2018, 0:00 UTC and has 41 bits. This caps
 * the useful lifespan of the generator to little less than 70 years (until ~2088). The sequence component
 * is 6 bits. If more than 64 IDs are requested in single millisecond, IDs will gracefully overflow to the next
 * millisecond and uniqueness is guaranteed in this case. The implementation does not allow overflowing
 * by more than 15 seconds, if IDs are requested at higher rate, the call will block. Note, however, that
 * clients are able to generate even faster because each call goes to a different (random) member and
 * the 64 IDs/ms limit is for single member.
 * <p>
 * <b>Node ID overflow</b>
 * <p>
 * Node ID component of the ID has 16 bits. Members with member list join version higher than
 * 2^16 won't be able to generate IDs, but functionality will be preserved by forwarding to another
 * member. It is possible to generate IDs on any member or client as long as there is at least one
 * member with join version smaller than 2^16 in the cluster. The remedy is to restart the cluster:
 * nodeId will be assigned from zero again. Uniqueness after the restart will be preserved thanks to
 * the timestamp component.
 *
 * @since 3.10
 */
public interface FlakeIdGenerator extends DistributedObject {

    /**
     * Generates and returns a cluster-wide unique ID.
     * <p>
     * Operation on member is always local, if the member has valid node ID, otherwise it's remote. On
     * client, this method goes to a random member and gets a batch of IDs, which will then be returned
     * locally for limited time. The pre-fetch size and the validity time can be configured for
     * each client and member, see {@link
     * com.hazelcast.config.Config#addFlakeIdGeneratorConfig(com.hazelcast.config.FlakeIdGeneratorConfig)
     * here} for member config and see {@code ClientConfig.addFlakeIdGeneratorConfig()} for client config.
     * <p>
     * <b>Note:</b> Values returned from this method may be not strictly ordered.
     *
     * @return new cluster-wide unique ID
     *
     * @throws HazelcastException if node ID for all members in the cluster is out of valid range.
     *      See "Node ID overflow" in {@link FlakeIdGenerator class documentation} for more details.
     */
    long newId();

}
