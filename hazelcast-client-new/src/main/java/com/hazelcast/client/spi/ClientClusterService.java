/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.MembershipListener;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;

import java.util.Collection;

/**
 * @author mdogan 5/16/13
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
    MemberImpl getMember(Address address);

    /**
     * Gets the member with the given uuid.
     *
     * @param uuid The uuid of the member.
     * @return The member that was found, or null if not found. If uuid is null, null is returned.
     */
    MemberImpl getMember(String uuid);

    /**
     * Gets the collection of members.
     *
     * @return The collection of members. Null will never be returned.
     */
    Collection<MemberImpl> getMemberList();

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
     * Returns the cluster-time.
     * <p/>
     *
     * @return The cluster-time.
     */
    long getClusterTime();

    /**
     * @param The listener to be registered.
     * @return The registration id.
     */
    String addMembershipListener(MembershipListener listener);

    /**
     * @param The registrationId of the listener to be removed.
     * @return true if successfully removed, false otherwise.
     */
    boolean removeMembershipListener(String registrationId);

    /**
     * The owner connection is opened to owner member of the client in the cluster.
     * If the owner member dies, other members of the cluster assumes this client is dead.
     *
     * @return The address of the owner connection.
     */
    Address getOwnerConnectionAddress();
}
