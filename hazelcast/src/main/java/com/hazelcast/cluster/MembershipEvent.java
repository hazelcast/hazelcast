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

import static java.lang.String.format;

/**
 * Membership event fired when a new member is added to the cluster and/or when a member leaves the cluster
 * and similar methods.
 *
 * @see MembershipListener
 */
@SuppressFBWarnings("SE_BAD_FIELD")
public class MembershipEvent extends EventObject {

    /**
     * This event type is fired when a new member joins the cluster.
     */
    public static final int MEMBER_ADDED = 1;

    /**
     * This event type is fired if a member left the cluster or was decided to be
     * unresponsive by other members for a extended time.
     */
    public static final int MEMBER_REMOVED = 2;

    private static final long serialVersionUID = -2010865371829087371L;

    protected Member member;

    protected Set<Member> members;

    private final int eventType;

    public MembershipEvent(Cluster cluster, Member member, int eventType, Set<Member> members) {
        super(cluster);
        this.member = member;
        this.eventType = eventType;
        this.members = members;
    }

    /**
     * Returns a consistent view of the the members immediately after this MembershipEvent has been processed. If a
     * member is removed, the returned set will not include this member. If a member is added, it will include
     * this member.
     * <p>
     * The problem with calling the {@link Cluster#getMembers()} method is that the content could already
     * have changed while processing this event, so it becomes very difficult to write a deterministic algorithm since
     * you cannot get a deterministic view of the members. This method solves that problem.
     * <p>
     * The set is immutable and ordered. For more information see {@link Cluster#getMembers()}.
     *
     * @return the members at the moment after this event.
     */
    public Set<Member> getMembers() {
        return members;
    }

    /**
     * Returns the cluster of the event.
     *
     * @return the current cluster instance
     */
    public Cluster getCluster() {
        return (Cluster) getSource();
    }

    /**
     * Returns the membership event type;
     * #MEMBER_ADDED
     * #MEMBER_REMOVED
     *
     * @return the membership event type
     */
    public int getEventType() {
        return eventType;
    }

    /**
     * Returns the removed or added member.
     *
     * @return member which is removed or added
     */
    public Member getMember() {
        return member;
    }

    @Override
    public String toString() {
        String type;
        switch (eventType) {
            case MEMBER_ADDED:
                type = "added";
                break;
            case MEMBER_REMOVED:
                type = "removed";
                break;
            default:
                throw new IllegalStateException();
        }

        return format("MembershipEvent {member=%s, type=%s, members=%s}", member, type, members);
    }
}
