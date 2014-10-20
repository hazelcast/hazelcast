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
 * Hazelcast cluster interface. It provides access to the members in the cluster and one can register for changes in the
 * cluster members.
 * <p/>
 * All the methods on the Cluster are thread-safe.
 */
public interface Cluster {

    /**
     * Adds MembershipListener to listen for membership updates.
     * <p/>
     * The addMembershipListener returns a register-id. This id is needed to remove the MembershipListener using the
     * {@link #removeMembershipListener(String)} method.
     * <p/>
     * If the MembershipListener implements the {@link InitialMembershipListener} interface, it will also receive
     * the {@link InitialMembershipEvent}.
     * <p/>
     * There is no check for duplicate registrations, so if you register the listener twice, it will get events twice.
     *
     * @param listener membership listener
     * @return the registration id.
     * @throws java.lang.NullPointerException if listener is null.
     * @see #removeMembershipListener(String)
     */
    String addMembershipListener(MembershipListener listener);

    /**
     * Removes the specified MembershipListener.
     * <p/>
     * If the same MembershipListener is registered multiple times, it needs to be removed multiple times.
     *
     * This method can safely be called multiple times for the same registration-id; every subsequent call is just ignored.
     *
     * @param registrationId the registrationId of MembershipListener to remove.
     * @return true if registration is removed, false otherwise.
     * @throws java.lang.NullPointerException if registration id is null.
     * @see #addMembershipListener(MembershipListener)
     */
    boolean removeMembershipListener(String registrationId);

    /**
     * Set of current members of the cluster. The returned set is an immutable set; so can't be modified.
     * <p/>
     * The returned set is backed by an ordered set. Every member in the cluster returns the 'members' in the same order.
     * To obtain the oldest member (the master) in the cluster, the first item in the set can be retrieved using
     * 'getMembers().iterator().next()'.
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
     * Cluster tries to keep a cluster-wide time which is might be different than the member's own system time.
     * Cluster-wide time is -almost- the same on all members of the cluster.
     *
     * @return cluster-wide time
     */
    long getClusterTime();
}
