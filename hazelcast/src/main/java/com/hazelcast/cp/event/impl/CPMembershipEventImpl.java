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

package com.hazelcast.cp.event.impl;

import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.event.CPMembershipEvent;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * Implementation of {@link CPMembershipEvent}.
 *
 * @since 4.1
 */
public class CPMembershipEventImpl implements CPMembershipEvent, IdentifiedDataSerializable {

    private EventType type;

    private CPMember member;

    public CPMembershipEventImpl() {
    }

    public CPMembershipEventImpl(CPMember member, EventType type) {
        this.type = type;
        this.member = member;
    }

    public CPMembershipEventImpl(CPMember member, byte eventType) {
        this.type = toEventType(eventType);
        this.member = member;
    }

    @Override
    public CPMember getMember() {
        return member;
    }

    @Override
    public EventType getType() {
        return type;
    }

    @Override
    public int getFactoryId() {
        return CpEventDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CpEventDataSerializerHook.MEMBERSHIP_EVENT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(member);
        out.writeString(type.name());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        member = in.readObject();
        type = EventType.valueOf(in.readString());
    }

    @Override
    public String toString() {
        return "CPMembershipEvent{" + "type=" + type + ", member=" + member + '}';
    }

    private static EventType toEventType(byte eventType) {
        switch (eventType) {
            case MEMBER_ADDED:
                return EventType.ADDED;
            case MEMBER_REMOVED:
                return EventType.REMOVED;
            default:
                throw new IllegalArgumentException("Unknown event type: " + eventType);
        }
    }
}
