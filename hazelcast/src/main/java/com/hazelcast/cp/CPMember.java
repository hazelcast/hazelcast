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

package com.hazelcast.cp;

import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.Endpoint;
import com.hazelcast.nio.Address;

/**
 * A CP member is a Hazelcast member that is internally elected to be part of
 * the {@link CPSubsystem}, hence maintain CP data structures. If
 * {@link CPSubsystemConfig#getCPMemberCount()} is configured to be N,
 * first N members of the cluster are assigned as CP members during startup.
 * After the CP subsystem is initialized, other Hazelcast members can be
 * promoted to be a CP member via the
 * {@link CPSubsystemManagementService#promoteToCPMember()} API.
 *
 * @see CPSubsystemConfig
 * @see CPSubsystemManagementService
 */
public interface CPMember extends Endpoint {

    /**
     * Returns the UUID of this CP member. The CP member UUID does not have to
     * be same with the uuid of the local member that is accessed via
     * {@link Cluster#getLocalMember()}.
     *
     * @return the UUID of this CP Member
     */
    String getUuid();

    /**
     * Returns the address of this CP member.
     * It is same with the address of {@link Cluster#getLocalMember()}
     *
     * @return the address of this CP member
     */
    Address getAddress();

}
