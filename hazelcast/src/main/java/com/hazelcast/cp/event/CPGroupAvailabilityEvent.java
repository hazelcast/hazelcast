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

package com.hazelcast.cp.event;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPMember;

import java.util.Collection;

/**
 * CPGroupAvailabilityEvent is published when a CP group's
 * availability is decreased or it has lost the majority completely.
 *
 * @since 4.1
 */
public interface CPGroupAvailabilityEvent {

    /**
     * Returns the id of the related CP group.
     *
     * @return CP group id
     */
    CPGroupId getGroupId();

    /**
     * Returns the current members of the CP group.
     *
     * @return group members
     */
    Collection<CPMember> getGroupMembers();

    /**
     * Returns the unavailable members of the CP group.
     *
     * @return unavailable members
     */
    Collection<CPMember> getUnavailableMembers();

    /**
     * Returns the majority member count of the CP group.
     * Simply it's {@code (memberCount/2) + 1}
     *
     * @return majority
     */
    int getMajority();

    /**
     * Returns whether this group has the majority of its members available or not.
     *
     * @return true if the group still has the majority, false otherwise
     */
    boolean isMajorityAvailable();

    /**
     * Returns whether this is the METADATA CP group or not.
     *
     * @return true if the group is METADATA group, false otherwise
     */
    boolean isMetadataGroup();
}
