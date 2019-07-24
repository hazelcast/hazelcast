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

package com.hazelcast.quorum;

import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MembershipListener;

import java.util.Collection;

/**
 * A function that can be used to conclude absence/presence of quorum.
 * The quorum function is consulted:<br>
 * <ul>
 *     <li>When a cluster membership change occurs (member added or removed)</li>
 *     <li>Whenever a hearbeat is received from a member, in case the {@code QuorumFunction} also
 *     implements {@link HeartbeatAware}</li>
 * </ul>
 *
 * A {@code QuorumFunction} that implements {@link HeartbeatAware} will be also notified of heartbeats.
 * Additionally, a {@code QuorumFunction} that implements {@link MembershipListener}
 * will be notified of membership events. All listener invocations are executed before the
 * {@code QuorumFunction}'s {@link #apply(Collection)} method is invoked.
 *
 * IMPORTANT: The term "quorum" simply refers to the count of members in the cluster required for an operation to succeed.
 * It does NOT refer to an implementation of Paxos or Raft protocols as used in many NoSQL and distributed systems.
 * The mechanism it provides in Hazelcast protects the user in case the number of nodes in a cluster drops below the
 * specified one.
 */
public interface QuorumFunction {

    /**
     * Determines if quorum is present based on the current list of members in the cluster.
     * Lite members are excluded and only data members are provided to this method.
     * <br>
     * This method should not block nor execute time-consuming operations, otherwise it may stall other quorum function
     * invocations.
     *
     * @param members snapshot of current member list
     * @return boolean presence/absence of quorum
     */
    boolean apply(Collection<Member> members);
}
