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

package com.hazelcast.cp;

import java.util.Collection;

/**
 * Contains information about a CP group.
 * <p>
 * A CP group is a set of CP members that run the Raft consensus algorithm
 * and contain CP data structure instances that are mapped to it.
 * <p>
 * There can be at most one active CP group with a given name.
 *
 * @see CPSubsystemManagementService
 */
public interface CPGroup {

    /**
     * Name of the internal CP group that is used for maintaining CP groups
     * and CP members
     */
    String METADATA_CP_GROUP_NAME = "METADATA";

    /**
     * Name of the DEFAULT CP group that is used when no group name is
     * specified while creating a CP data structure proxy.
     */
    String DEFAULT_GROUP_NAME = "default";

    /**
     * Represents status of a CP group
     */
    enum CPGroupStatus {
        /**
         * A CP group is active after it is initialized with the first request
         * sent to it, and before its destroy process is initialized.
         */
        ACTIVE,

        /**
         * A CP group switches to this state after its destroy process is
         * initialized, but not completed yet.
         */
        DESTROYING,

        /**
         * A CP group switches to this state after its destroy process is
         * completed.
         */
        DESTROYED
    }

    /**
     * Returns unique id of the CP group
     */
    CPGroupId id();

    /**
     * Returns status of the CP group
     */
    CPGroupStatus status();

    /**
     * Returns current members of the CP group
     */
    Collection<CPMember> members();

}
