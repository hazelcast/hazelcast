/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.spi;

import com.hazelcast.core.Client;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberSelector;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.nio.Address;

import java.util.Collection;

/**
 * Cluster service for Hazelcast clients.
 *
 * Allows to retrieve Hazelcast members of the cluster, e.g. by their {@link Address} or uuid.
 */
public interface ClientClusterService {

    /**
     * @return The client interface representing the local client.
     */
    Client getLocalClient();

    /**
     * Gets the member for the given address.
     *
     * @param address The address of the member to look up.
     * @return The member that was found, or null if not found. If address is null, null is returned.
     */
    Member getMember(Address address);

    /**
     * Gets the member with the given uuid.
     *
     * @param uuid The uuid of the member.
     * @return The member that was found, or null if not found. If uuid is null, null is returned.
     */
    Member getMember(String uuid);

    /**
     * Gets the collection of members.
     *
     * @return The collection of members. Null will never be returned.
     */
    Collection<Member> getMemberList();

    /**
     * Returns a collection of the members that satisfy the given {@link com.hazelcast.core.MemberSelector}.
     *
     * @param selector {@link com.hazelcast.core.MemberSelector} instance to filter members to return
     * @return members that satisfy the given {@link com.hazelcast.core.MemberSelector}.
     */
    Collection<Member> getMembers(MemberSelector selector);

    /**
     * Returns the address of the master member.
     *
     * @return The address of the master member. Could be null if the master is not yet known.
     */
    Address getMasterAddress();

    /**
     * Gets the current number of members.
     *
     * @return The current number of members.
     */
    int getSize();

    /**
     * Gets the number of members that satisfy the given {@link com.hazelcast.core.MemberSelector} instance.
     * @param selector {@link com.hazelcast.core.MemberSelector} instance that filters members to be counted.
     * @return the number of members that satisfy the given {@link com.hazelcast.core.MemberSelector} instance.
     */
    int getSize(MemberSelector selector);

    /**
     * Returns the cluster-time.
     *
     * @return The cluster-time.
     */
    long getClusterTime();

    /**
     * @param listener The listener to be registered.
     * @return The registration ID
     */
    String addMembershipListener(MembershipListener listener);

    /**
     * @param registrationId The registrationId of the listener to be removed.
     * @return true if successfully removed, false otherwise.
     */
    boolean removeMembershipListener(String registrationId);

}
