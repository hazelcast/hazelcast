/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cp.CPGroup;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.event.CPGroupAvailabilityEvent;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.readCollection;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeCollection;

// it's probably easier to just add a flag to the existing availability message, e.g. isShutdown -- default value of false
public class CPGroupAvailabilityEventGracefulImpl implements CPGroupAvailabilityEvent, IdentifiedDataSerializable {
    private CPGroupId groupId;
    private Collection<CPMember> members;
    private Collection<CPMember> unavailableMembers;

    public CPGroupAvailabilityEventGracefulImpl() {
    }

    public CPGroupAvailabilityEventGracefulImpl(CPGroupId groupId, Collection<CPMember> members,
                                                Collection<CPMember> unavailableMembers) {
        this.groupId = groupId;
        this.members = members;
        this.unavailableMembers = unavailableMembers;
    }

    @Override
    public CPGroupId getGroupId() {
        return groupId;
    }

    @Override
    public Collection<CPMember> getGroupMembers() {
        return members;
    }

    @Override
    public Collection<CPMember> getUnavailableMembers() {
        return unavailableMembers;
    }

    @Override
    public int getMajority() {
        return members.size() / 2 + 1;
    }

    @Override
    public boolean isMajorityAvailable() {
        return unavailableMembers.size() < getMajority();
    }

    @Override
    public boolean isMetadataGroup() {
        return CPGroup.METADATA_CP_GROUP_NAME.equals(groupId.getName());
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(groupId);
        writeCollection(members, out);
        writeCollection(unavailableMembers, out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        groupId = in.readObject();
        members = readCollection(in);
        unavailableMembers = readCollection(in);
    }

    @Override
    public int getFactoryId() {
        return CpEventDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CpEventDataSerializerHook.GROUP_AVAILABILITY_GRACEFUL_EVENT;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CPGroupAvailabilityEventGracefulImpl that = (CPGroupAvailabilityEventGracefulImpl) o;
        return Objects.equals(groupId, that.groupId) && Objects.equals(members, that.members) && Objects.equals(
                unavailableMembers, that.unavailableMembers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupId, members, unavailableMembers);
    }

    @Override
    public String toString() {
        return "CPGroupAvailabilityEventGracefulImpl{" + "groupId=" + groupId + ", members=" + members + ", unavailableMembers="
                       + unavailableMembers + '}';
    }
}
