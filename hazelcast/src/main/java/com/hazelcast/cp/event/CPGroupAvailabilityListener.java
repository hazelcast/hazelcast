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

import java.util.EventListener;

/**
 * CPGroupAvailabilityListener is notified when availability
 * of a CP group decreases or it loses the majority completely.
 * <p>
 * In general, availability decreases when a CP member becomes unreachable
 * because of process crash, network partition, out of memory etc.
 * Once a member is declared as unavailable by the Hazelcast's failure detector,
 * that member is removed from the cluster. If it is also a CP member,
 * {@link CPGroupAvailabilityEvent}s are fired for each
 * CP group that member belongs to.
 * <p>
 * As a special case, CPGroupAvailabilityListener has a separate method
 * to report loss of majority. When majority of a CP group is lost,
 * that CP group cannot make progress anymore. Even a new CP member cannot join
 * to this CP group, because membership changes also go through the Raft consensus
 * algorithm. When a CP group has lost its majority:
 * <ul>
 * <li>
 * If the group is a non-METADATA CP group, it must be force-destroyed immediately,
 * because it can block the METADATA CP group to perform membership changes on CP Subsystem.
 * </li>
 * <li>
 * If the majority of the METADATA CP group permanently crash, unfortunately
 * it is equivalent to the permanent crash of the majority CP members of the whole CP Subsystem,
 * even though other CP groups are running fine.
 * </li>
 * </ul>
 *
 * @see com.hazelcast.cp.CPSubsystem
 * @see com.hazelcast.cp.CPSubsystemManagementService
 * @see com.hazelcast.cp.CPSubsystem#addGroupAvailabilityListener(CPGroupAvailabilityListener)
 * @since 4.1
 */
public interface CPGroupAvailabilityListener extends EventListener {

    /**
     * Called when a CP group's availability decreases,
     * but still has the majority of members available.
     *
     * @param event CP group availability event
     */
    void availabilityDecreased(CPGroupAvailabilityEvent event);

    /**
     * Called when a CP group has lost its majority.
     *
     * @param event CP group availability event
     */
    void majorityLost(CPGroupAvailabilityEvent event);
}
