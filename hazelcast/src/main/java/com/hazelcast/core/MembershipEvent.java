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

import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

/**
 * Membership event fired when a new member is added
 * to the cluster and/or when a member leaves the cluster.
 *
 * @see MembershipListener
 */
public class MembershipEvent implements DataSerializable {

    private static final long serialVersionUID = -2010865371829087371L;

    public static final int MEMBER_ADDED = 1;

    public static final int MEMBER_REMOVED = 3;

    private Member member;

    private int eventType;

    public MembershipEvent() {
    }

    public MembershipEvent(Member member, int eventType) {
        this.member = member;
        this.eventType = eventType;
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

    public void writeData(ObjectDataOutput out) throws IOException {
        member.writeData(out);
        out.writeInt(eventType);
    }

    public void readData(ObjectDataInput in) throws IOException {
        member = new MemberImpl();
        member.readData(in);
        eventType = in.readInt();
    }

    @Override
    public String toString() {
        return "MembershipEvent {" + member + "} "
                + ((eventType == MEMBER_ADDED) ? "added" : "removed");
    }
}
