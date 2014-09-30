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

package com.hazelcast.cluster;

import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.CoreService;

import java.util.Collection;

/**
 * A service responsible for member related functionality. So members joining, leaving etc.
 */
public interface ClusterService extends CoreService {

    MemberImpl getMember(Address address);

    MemberImpl getMember(String uuid);

    Collection<MemberImpl> getMemberList();

    /**
     * Returns a collection of all members part of the cluster.
     *
     * @return all members that are part of the cluster.
     */
    Collection<Member> getMembers();

    /**
     * Returns the address of the master member.
     *
     * @return the address of the master member. Could be null if the master is not yet known.
     */
    Address getMasterAddress();

    /**
     * Checks if this member is the master.
     *
     * @return true if master, false otherwise.
     */
    boolean isMaster();

    /**
     * Gets the address of this member.
     *
     * @return the address of this member. The returned value will never be null.
     */
    Address getThisAddress();

    /**
     * Gets the current number of members.
     *
     * @return the current number of members.
     */
    int getSize();

    /**
     * Returns the cluster-time.
     *
     * TODO: Needs to be improved.
     *
     * @return the cluster-time.
     */
    long getClusterTime();
}
