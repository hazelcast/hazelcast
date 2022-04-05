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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.cluster.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.readList;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeList;
import static java.lang.Math.max;
import static java.util.Collections.unmodifiableList;

/**
 * MembersView is a container object to carry member list and version together.
 */
public final class MembersView implements IdentifiedDataSerializable {

    private int version;
    private List<MemberInfo> members;

    public MembersView() {
    }

    public MembersView(int version, List<MemberInfo> members) {
        this.version = version;
        this.members = members;
    }

    /**
     * Creates clone of source {@link MembersView} additionally including new members.
     *
     * @param source     source map
     * @param newMembers new members to add
     * @return clone map
     */
    static MembersView cloneAdding(MembersView source, Collection<MemberInfo> newMembers) {
        List<MemberInfo> list = new ArrayList<>(source.size() + newMembers.size());
        list.addAll(source.getMembers());
        int newVersion = max(source.version, source.size());
        for (MemberInfo newMember : newMembers) {
            MemberInfo m = new MemberInfo(newMember.getAddress(), newMember.getUuid(), newMember.getAttributes(),
                    newMember.isLiteMember(), newMember.getVersion(), ++newVersion, newMember.getAddressMap());
            list.add(m);
        }

        return new MembersView(newVersion, unmodifiableList(list));
    }

    /**
     * Creates a new {@code MemberMap} including given members.
     *
     * @param version version
     * @param members members
     * @return a new {@code MemberMap}
     */
    public static MembersView createNew(int version, Collection<MemberImpl> members) {
        List<MemberInfo> list = new ArrayList<>(members.size());

        for (MemberImpl member : members) {
            list.add(new MemberInfo(member));
        }

        return new MembersView(version, unmodifiableList(list));
    }

    public List<MemberInfo> getMembers() {
        return members;
    }

    public int size() {
        return members.size();
    }

    public int getVersion() {
        return version;
    }

    MemberMap toMemberMap() {
        MemberImpl[] m = new MemberImpl[size()];
        int ix = 0;
        for (MemberInfo memberInfo : members) {
            m[ix++] = memberInfo.toMember();
        }
        return MemberMap.createNew(version, m);
    }

    public boolean containsAddress(Address address) {
        for (MemberInfo member : members) {
            if (member.getAddress().equals(address)) {
                return true;
            }
        }

        return false;
    }

    public boolean containsMember(Address address, UUID uuid) {
        for (MemberInfo member : members) {
            if (member.getAddress().equals(address)) {
                return member.getUuid().equals(uuid);
            }
        }

        return false;
    }

    public Set<Address> getAddresses() {
        Set<Address> addresses = new HashSet<>(members.size());
        for (MemberInfo member : members) {
            addresses.add(member.getAddress());
        }
        return addresses;
    }

    public MemberInfo getMember(Address address) {
        for (MemberInfo member : members) {
            if (member.getAddress().equals(address)) {
                return member;
            }
        }

        return null;
    }

    public boolean isLaterThan(MembersView other) {
        return version > other.version;
    }

    @Override
    public int getFactoryId() {
        return ClusterDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ClusterDataSerializerHook.MEMBERS_VIEW;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(version);
        writeList(members, out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        version = in.readInt();
        this.members = unmodifiableList(readList(in));
    }

    @Override
    public String toString() {
        return "MembersView{" + "version=" + version + ", members=" + members + '}';
    }

}
