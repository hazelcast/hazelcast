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

import com.hazelcast.cp.CPGroup;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.event.CPGroupAvailabilityEvent;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.readCollection;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeCollection;

/**
 * Implementation of {@link CPGroupAvailabilityEvent}.
 *
 * @since 4.1
 */
public class CPGroupAvailabilityEventImpl implements CPGroupAvailabilityEvent, IdentifiedDataSerializable {

    private CPGroupId groupId;
    private Collection<CPMember> members;
    private Collection<CPMember> missingMembers;

    public CPGroupAvailabilityEventImpl() {
    }

    public CPGroupAvailabilityEventImpl(CPGroupId groupId, Collection<CPMember> members, Collection<CPMember> missingMembers) {
        this.groupId = groupId;
        this.members = members;
        this.missingMembers = missingMembers;
    }

    @Override
    public CPGroupId getGroupId() {
        return groupId;
    }

    @Override
    public Collection<CPMember> getUnavailableMembers() {
        return Collections.unmodifiableCollection(missingMembers);
    }

    @Override
    public Collection<CPMember> getGroupMembers() {
        return Collections.unmodifiableCollection(members);
    }

    @Override
    public int getMajority() {
        return members.size() / 2 + 1;
    }

    @Override
    public boolean isMajorityAvailable() {
        return missingMembers.size() < getMajority();
    }

    @Override
    public boolean isMetadataGroup() {
        return CPGroup.METADATA_CP_GROUP_NAME.equals(groupId.getName());
    }

    @Override
    public int getFactoryId() {
        return CpEventDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CpEventDataSerializerHook.GROUP_AVAILABILITY_EVENT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(groupId);
        writeCollection(members, out);
        writeCollection(missingMembers, out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        groupId = in.readObject();
        members = readCollection(in);
        missingMembers = readCollection(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CPGroupAvailabilityEventImpl that = (CPGroupAvailabilityEventImpl) o;
        if (!groupId.equals(that.groupId)) {
            return false;
        }
        // Regardless of collection type and collection order,
        // if two collections have the same elements,
        // events are considered equal.
        if (members.size() != that.members.size()) {
            return false;
        }
        if (missingMembers.size() != that.missingMembers.size()) {
            return false;
        }
        if (!missingMembers.containsAll(that.missingMembers)) {
            return false;
        }
        return members.containsAll(that.members);
    }

    @Override
    public int hashCode() {
        int result = groupId.hashCode();
        for (CPMember member : members) {
            result = 31 * result + member.hashCode();
        }
        for (CPMember member : missingMembers) {
            result = 31 * result + member.hashCode();
        }
        return result;
    }

    @Override
    public String toString() {
        return "CPGroupAvailabilityEvent{" + "groupId=" + groupId + ", members=" + members + ", missingMembers="
                + missingMembers + '}';
    }
}
