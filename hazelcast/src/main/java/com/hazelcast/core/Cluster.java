/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import java.util.Set;

/**
 * Hazelcast cluster interface.
 */
public interface Cluster {

    /**
     * Adds MembershipListener to listen for membership updates.
     *
     * If the MembershipListener implements the {@link InitialMembershipListener} interface, it will also receive
     * the {@link InitialMembershipEvent}.
     *
     * @param listener membership listener
     * @return returns registration id.
     */
    String addMembershipListener(MembershipListener listener);

    /**
     * Removes the specified membership listener.
     *
     *
     * @param registrationId Id of listener registration.
     *
     * @return true if registration is removed, false otherwise
     */
    boolean removeMembershipListener(final String registrationId);

    /**
     * Set of current members of the cluster.
     * Returning set instance is not modifiable.
     * Every member in the cluster has the same member list in the same
     * order. First member is the oldest member.
     *
     * @return current members of the cluster
     */
    Set<Member> getMembers();

    /**
     * Returns this Hazelcast instance member
     *
     * @return this Hazelcast instance member
     */
    Member getLocalMember();

    /**
     * Returns the cluster-wide time in milliseconds.
     * <p/>
     * Cluster tries to keep a cluster-wide time which is
     * might be different than the member's own system time.
     * Cluster-wide time is -almost- the same on all members
     * of the cluster.
     *
     * @return cluster-wide time
     */
    long getClusterTime();
}
