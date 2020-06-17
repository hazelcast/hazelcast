/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import java.util.EventListener;

/**
 * Listener to get notified when a quorum state is changed
 * <p>
 * IMPORTANT: The term "quorum" simply refers to the count of members in the cluster required for an operation to succeed.
 * It does NOT refer to an implementation of Paxos or Raft protocols as used in many NoSQL and distributed systems.
 * The mechanism it provides in Hazelcast protects the user in case the number of nodes in a cluster drops below the
 * specified one.
 * <p>
 * {@code QuorumEvent}s are fired only after the quorum is met for the first time.
 * For instance, see the following scenario for a quorum size is equal to 3:
 * <ul>
 * <li>Member-1 starts; no quorum events are fired, since quorum is not present yet.</li>
 * <li>Member-2 starts; no quorum events are fired, since quorum is not present yet.</li>
 * <li>Member-3 starts; no quorum events, since this is the first time quorum is met.</li>
 * <li>Member-1 stops; both Member-2 and Member-3 fire quorum absent events,
 * since member count drops below 3.</li>
 * <li>Member-1 restarts; both Member-2 and Member-3 fire quorum present events,
 * but Member-1 does not, because for Member-1 this is the first time quorum is met.</li>
 * </ul>
 */
public interface QuorumListener extends EventListener {

    /**
     * Called when quorum presence state is changed.
     *
     * @param quorumEvent provides information about quorum presence and current member list.
     */
    void onChange(QuorumEvent quorumEvent);

}
