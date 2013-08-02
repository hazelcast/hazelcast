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

import java.util.EventObject;
import java.util.Set;

/**
 * Membership event fired when a new member is added
 * to the cluster and/or when a member leaves the cluster.
 *
 * @see MembershipListener
 */
public class MembershipEvent extends EventObject {

    private static final long serialVersionUID = -2010865371829087371L;

    public static final int MEMBER_ADDED = 1;

    public static final int MEMBER_REMOVED = 2;

    private final Member member;

    private final int eventType;

    private final Set<Member> members;

    public MembershipEvent(Cluster cluster, Member member, int eventType, Set<Member> members) {
        super(cluster);
        this.member = member;
        this.eventType = eventType;
        this.members = members;
    }

    /**
     * Returns a consistent view of the the members exactly after this MembershipEvent has been processed. So if a
     * member is removed, the returned set will not include this member. And if a member is added it will include
     * this member.
     *
     * The problem with calling the {@link com.hazelcast.core.Cluster#getMembers()} is that the content could already
     * have changed while processing this event so it becomes very difficult to write a deterministic algorithm since
     * you can't get a deterministic view of the members. This method solves that problem.
     *
     * The set is immutable and ordered. For more information see {@link com.hazelcast.core.Cluster#getMembers()}.
     *
     * @return the members at the moment after this event.
     */
    public Set<Member> getMembers() {
        return members;
    }

    /**
     * Returns the cluster of the event.
     *
     * @return
     */
    public Cluster getCluster() {
        return (Cluster) getSource();
    }

    /**
     * Returns the membership event type; #MEMBER_ADDED or #MEMBER_REMOVED
     *
     * @return the membership event type
     */
    public int getEventType() {
        return eventType;
    }

    /**
     * Returns the removed or added member.
     *
     * @return member which is removed/added
     */
    public Member getMember() {
        return member;
    }

    @Override
    public String toString() {
        return "MembershipEvent {" + member + "} "
                + ((eventType == MEMBER_ADDED) ? "added" : "removed");
    }
}
