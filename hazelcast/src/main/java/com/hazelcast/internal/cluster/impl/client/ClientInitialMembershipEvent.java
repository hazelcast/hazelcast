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

package com.hazelcast.internal.cluster.impl.client;

import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

public final class ClientInitialMembershipEvent implements IdentifiedDataSerializable {

    public static final int MEMBER_ADDED = MembershipEvent.MEMBER_ADDED;

    public static final int MEMBER_REMOVED = MembershipEvent.MEMBER_REMOVED;

    public static final int INITIAL_MEMBERS = 3;

    public static final int MEMBER_ATTRIBUTE_CHANGED = MembershipEvent.MEMBER_ATTRIBUTE_CHANGED;

    private Member member;

    private MemberAttributeChange memberAttributeChange;

    private int eventType;

    private Collection<MemberImpl> memberList;

    public ClientInitialMembershipEvent() {
    }

    public ClientInitialMembershipEvent(Member member, int eventType) {
        this.member = member;
        this.eventType = eventType;
    }

    public ClientInitialMembershipEvent(Member member, MemberAttributeChange memberAttributeChange) {
        this.member = member;
        this.eventType = MEMBER_ATTRIBUTE_CHANGED;
        this.memberAttributeChange = memberAttributeChange;
    }

    public ClientInitialMembershipEvent(Collection<MemberImpl> members) {
        this.eventType = INITIAL_MEMBERS;
        memberList = members;
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

    /**
     * Returns the member attribute chance operation to execute
     * if event type is {@link #MEMBER_ATTRIBUTE_CHANGED}.
     *
     * @return MemberAttributeChange to execute
     */
    public MemberAttributeChange getMemberAttributeChange() {
        return memberAttributeChange;
    }

    /**
     * Returns member list after initial registration. Note this method returns null if event type is not INITIAL_MEMBERS
     *
     * @return set of members at the moment of registration
     */
    public Collection<MemberImpl> getMembers() {
        if (eventType != INITIAL_MEMBERS) {
            return null;
        }

        return memberList;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(eventType);
        if (eventType == INITIAL_MEMBERS) {

            out.writeInt(memberList.size());
            for (MemberImpl member : memberList) {
                member.writeData(out);
            }

            return;
        }
        member.writeData(out);
        if (eventType == MEMBER_ATTRIBUTE_CHANGED) {
            memberAttributeChange.writeData(out);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        eventType = in.readInt();
        if (eventType == INITIAL_MEMBERS) {
            int memberListSize = in.readInt();
            memberList = new LinkedList<MemberImpl>();
            for (int i = 0; i < memberListSize; i++) {
                MemberImpl m = new MemberImpl();
                m.readData(in);
                memberList.add(m);
            }
            return;
        }

        member = new MemberImpl();
        member.readData(in);

        if (eventType == MEMBER_ATTRIBUTE_CHANGED) {
            memberAttributeChange = new MemberAttributeChange();
            memberAttributeChange.readData(in);
        }
    }

    @Override
    public int getFactoryId() {
        return ClusterDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ClusterDataSerializerHook.INITIAL_MEMBERSHIP_EVENT;
    }
}
