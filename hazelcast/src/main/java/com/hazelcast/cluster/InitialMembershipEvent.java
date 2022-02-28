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

package com.hazelcast.cluster;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.EventObject;
import java.util.Set;

/**
 * An event that is sent when a {@link InitialMembershipListener} registers itself on a {@link Cluster}. For more
 * information, see the {@link InitialMembershipListener}.
 *
 * @author Peter Veentjer
 * @see InitialMembershipListener
 * @see MembershipListener
 * @see MembershipEvent
 */
@SuppressFBWarnings("SE_BAD_FIELD")
public class InitialMembershipEvent extends EventObject {

    private static final long serialVersionUID = -2010865371829087371L;

    private final Set<Member> members;

    public InitialMembershipEvent(Cluster cluster, Set<Member> members) {
        super(cluster);
        this.members = members;
    }

    /**
     * Returns an immutable set of ordered members at the moment this {@link InitialMembershipListener} is
     * registered. See {@link Cluster#getMembers()} for more information.
     *
     * @return an immutable set of ordered members.
     */
    public Set<Member> getMembers() {
        return members;
    }

    /**
     * Returns the cluster of the event.
     *
     * @return the cluster of the event.
     */
    public Cluster getCluster() {
        return (Cluster) getSource();
    }

    @Override
    public String toString() {
        return "InitialMembershipEvent {" + members + "}";
    }
}
