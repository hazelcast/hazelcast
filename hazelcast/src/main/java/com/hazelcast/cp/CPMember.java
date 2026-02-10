/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp;

import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.Address;

import java.util.UUID;

/**
 * A CP member is a Hazelcast member that is internally elected to be part of
 * the {@link CPSubsystem}, hence maintain CP data structures. If
 * {@link CPSubsystemConfig#getCPMemberCount()} is configured to be N,
 * first N members of the cluster are assigned as CP members during startup.
 * After CP Subsystem is initialized, other Hazelcast members can be promoted
 * to be a CP member via
 * {@link CPSubsystemManagementService#promoteToCPMember()}.
 *
 * @see CPSubsystem
 * @see CPSubsystemConfig
 * @see CPSubsystemManagementService
 */
public interface CPMember {

    /**
     * Returns the UUID of this CP member. The CP member UUID does not have to
     * be same with the UUID of the local member that is accessed via
     * {@link Cluster#getLocalMember()}.
     * <p>
     * This UUID is used as the primary identifier of CP members. It cannot be
     * changed without impacting the identity of this member in the CP subsystem.
     *
     * @return the UUID of this CP Member
     */
    UUID getUuid();

    /**
     * Returns the address of this CP member.
     * It is same with the address of {@link Cluster#getLocalMember()}.
     * <p>
     * This Address can be changed, and the CP member will retain its identity,
     * as the UUID is used to identify uniqueness in the CP subsystem. See
     * {@code RaftService#replaceLocalMemberIfAddressChanged}.
     *
     * @return the address of this CP member
     */
    Address getAddress();

    /**
     * Returns whether this CP member is configured to automatically step down
     * from leadership in non-metadata CP groups.
     *
     * <p>If {@code true}, whenever this member is elected leader, it will suspend
     * replication, immediately trigger a leadership transfer, and resume normal
     * operation once a leader-capable member takes over. Client operations are
     * retried transparently and eventually succeed under the new leader.</p>
     *
     * <p>When this flag is enabled, the member's effective priority is treated as
     * {@link Integer#MIN_VALUE} to prevent leader rebalancing from assigning
     * leadership to a node that would immediately step down.</p>
     *
     * <p>This feature is useful when a node has high latency to the rest of the
     * CP group, where holding leadership could destabilize replication. A brief
     * unavailability window may occur during the leadership transfer, proportional
     * to the RTT and log catch-up required for the target leader.</p>
     *
     * @return {@code true} if this member automatically steps down from leadership
     * @since 5.7
     */
    boolean isAutoStepDownWhenLeader();

}
