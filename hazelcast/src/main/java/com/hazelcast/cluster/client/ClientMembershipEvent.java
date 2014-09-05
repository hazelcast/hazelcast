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

package com.hazelcast.cluster.client;

import com.hazelcast.cluster.ClusterDataSerializerHook;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

public final class ClientMembershipEvent implements IdentifiedDataSerializable {

    public static final int MEMBER_ADDED = MembershipEvent.MEMBER_ADDED;

    public static final int MEMBER_REMOVED = MembershipEvent.MEMBER_REMOVED;

    public static final int MEMBER_ATTRIBUTE_CHANGED = MembershipEvent.MEMBER_ATTRIBUTE_CHANGED;

    private Member member;

    private MemberAttributeChange memberAttributeChange;

    private int eventType;

    public ClientMembershipEvent() {
    }

    public ClientMembershipEvent(Member member, int eventType) {
        this(member, null, eventType);
    }

    public ClientMembershipEvent(Member member, MemberAttributeChange memberAttributeChange) {
        this(member, memberAttributeChange, MEMBER_ATTRIBUTE_CHANGED);
    }

    private ClientMembershipEvent(Member member, MemberAttributeChange memberAttributeChange, int eventType) {
        this.member = member;
        this.eventType = eventType;
        this.memberAttributeChange = memberAttributeChange;
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

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        member.writeData(out);
        out.writeInt(eventType);
        out.writeBoolean(memberAttributeChange != null);
        if (memberAttributeChange != null) {
            memberAttributeChange.writeData(out);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        member = new MemberImpl();
        member.readData(in);
        eventType = in.readInt();
        if (in.readBoolean()) {
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
        return ClusterDataSerializerHook.MEMBERSHIP_EVENT;
    }
}
