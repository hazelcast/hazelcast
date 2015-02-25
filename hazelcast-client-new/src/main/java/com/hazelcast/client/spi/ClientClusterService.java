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
     * @return Client interface representing local client
     */
    Client getLocalClient();

    /**
     * Gets the member for the given address.
     *
     * @param address the address of the member to lookup.
     * @return the found member, or null if not found. If address is null, null is returned.
     */
    MemberImpl getMember(Address address);

    /**
     * Gets the member with the given uuid.
     *
     * @param uuid the uuid of the member
     * @return the found member, or null if not found. If uuid is null, null is returned.
     */
    MemberImpl getMember(String uuid);

    /**
     * Gets the collection of members.
     *
     * @return the collection of member. Null will never be returned.
     */
    Collection<MemberImpl> getMemberList();

    /**
     * Returns the address of the master member.
     *
     * @return the address of the master member. Could be null if the master is not yet known.
     */
    Address getMasterAddress();

    /**
     * Gets the current number of members.
     *
     * @return the current number of members.
     */
    int getSize();

    /**
     * Returns the cluster-time.
     * <p/>
     *
     * @return the cluster-time.
     */
    long getClusterTime();

    /**
     * @param listener to be registered
     * @return registration id
     */
    String addMembershipListener(MembershipListener listener);

    /**
     * @param registrationId of listener
     * @return true if successfully removed, false otherwise
     */
    boolean removeMembershipListener(String registrationId);

}
