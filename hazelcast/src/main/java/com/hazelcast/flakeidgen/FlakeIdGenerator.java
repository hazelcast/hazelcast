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

package com.hazelcast.flakeidgen;

import com.hazelcast.config.FlakeIdGeneratorConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.cluster.ClusterService;

/**
 * A cluster-wide unique ID generator. Generated IDs are {@code long} primitive values and are
 * k-ordered (roughly ordered). IDs are in the range from {@code 0} to {@code Long.MAX_VALUE} (they
 * can be negative in {@linkplain FlakeIdGeneratorConfig#setEpochStart(long) a special case}).
 *
 * <p>The IDs contain a timestamp component and a node ID component, which is assigned when the
 * member joins the cluster. This allows the IDs to be ordered and unique without any coordination
 * between members, which makes the generator safe even in split-brain scenario (for caveats,
 * {@linkplain ClusterService#getMemberListJoinVersion() see here}).
 *
 * <p>The layout of the generate IDs is as follows (starting from most significant bits):<ul>
 *     <li>timestamp bits (41 by default)
 *     <li>sequence bits (6 by default)
 *     <li>node ID bits (16 by default)
 * </ul>
 *
 * You can configure the number of {@linkplain FlakeIdGeneratorConfig#setBitsSequence(int) sequence
 * bits} and of {@linkplain FlakeIdGeneratorConfig#setBitsNodeId(int) node ID bits}. Timestamp bits
 * are assigned in a way that all three components add to 63. It is an error to assign sequence and
 * node ID bits that exceed 63.
 *
 * <p>You can tweak bits assigned to each component according to your use case using {@link
 * FlakeIdGeneratorConfig}:
 *
 * <p><b>Timestamp bits</b>
 *
 * <p>The number of bits assigned to this component determines the lifespan of the generator. For
 * example the 41 bits assigned by default allow the generator to generate IDs during about 70 years
 * (or 140 years, if you can tolerate negative IDs). Along with {@linkplain
 * FlakeIdGeneratorConfig#setEpochStart(long) epoch start}, which is at the beginning of 2018 by
 * default and is also configurable, you can determine when the generator will run out. By default
 * it runs out slightly before the end of 2088.
 *
 * <p><b>Sequence bits</b>
 *
 * <p>The number of bits assigned to the sequence component determines how many IDs can be generated
 * per millisecond. The default value of 6, for example, allows to generate 64 IDs per millisecond
 * (2^6). That is 64000 IDs per second. If you don't need this many IDs, you can reduce the sequence
 * bits and allocate more for node ID or timestamp.
 *
 * <p>If more than 64 IDs are requested in a single millisecond, IDs will gracefully overflow to the
 * next millisecond and uniqueness is guaranteed in this case. The implementation does not allow
 * overflowing by more than certain number of seconds, {@linkplain
 * FlakeIdGeneratorConfig#setAllowedFutureMillis(long) 15 by default}; if IDs are requested at a
 * higher rate, the call will block. Note, however, that clients might be able to generate faster
 * because each call goes to a different (random) member and the 64 IDs/ms limit is for a single
 * member.
 *
 * <p><b>Node ID bits</b>
 *
 * <p>This component determines mostly how many changes in the cluster we can tolerate. Default node
 * ID component has 16 bits. Members with member list join version higher than 2^16 won't be able to
 * generate IDs, but functionality will be preserved by forwarding to another member. It is possible
 * to generate IDs on any member or client as long as there is at least one member with join version
 * smaller than 2^16 in the cluster. The only remedy is to restart the cluster: nodeId will be
 * assigned from zero again. Uniqueness after the restart will be preserved thanks to the timestamp
 * component.
 *
 * @since 3.10
 */
public interface FlakeIdGenerator extends DistributedObject {

    /**
     * Generates and returns a cluster-wide unique ID.
     * <p>
     * Operation on a member is always local, if the member has valid node ID, otherwise it's remote. On
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
     * @throws HazelcastException thrown if:<ul>
     *     <li>node ID for all members in the cluster is out of valid range.
     *        See "Node ID overflow" in {@linkplain FlakeIdGenerator class documentation} for more details.
     *     <li>if the lifespan of the generator is over
     *   </ul>
     */
    long newId();

}
