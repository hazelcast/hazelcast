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

import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.cluster.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.UUID;

public class MembersViewMetadata implements IdentifiedDataSerializable {

    private Address memberAddress;

    private UUID memberUuid;

    private Address masterAddress;

    private int memberListVersion;

    public MembersViewMetadata() {
    }

    public MembersViewMetadata(Address memberAddress, UUID memberUuid, Address masterAddress, int memberListVersion) {
        this.memberAddress = memberAddress;
        this.memberUuid = memberUuid;
        this.masterAddress = masterAddress;
        this.memberListVersion = memberListVersion;
    }

    public Address getMemberAddress() {
        return memberAddress;
    }

    public UUID getMemberUuid() {
        return memberUuid;
    }

    public Address getMasterAddress() {
        return masterAddress;
    }

    public int getMemberListVersion() {
        return memberListVersion;
    }

    @Override
    public int getFactoryId() {
        return ClusterDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ClusterDataSerializerHook.MEMBERS_VIEW_METADATA;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(memberAddress);
        UUIDSerializationUtil.writeUUID(out, memberUuid);
        out.writeObject(masterAddress);
        out.writeInt(memberListVersion);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        memberAddress = in.readObject();
        memberUuid = UUIDSerializationUtil.readUUID(in);
        masterAddress = in.readObject();
        memberListVersion = in.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MembersViewMetadata that = (MembersViewMetadata) o;

        if (memberListVersion != that.memberListVersion) {
            return false;
        }
        if (!memberAddress.equals(that.memberAddress)) {
            return false;
        }
        if (!memberUuid.equals(that.memberUuid)) {
            return false;
        }
        return masterAddress.equals(that.masterAddress);
    }

    @Override
    public int hashCode() {
        int result = memberAddress.hashCode();
        result = 31 * result + memberUuid.hashCode();
        result = 31 * result + masterAddress.hashCode();
        result = 31 * result + memberListVersion;
        return result;
    }

    @Override
    public String toString() {
        return "MembersViewMetadata{" + "address=" + memberAddress + ", memberUuid='" + memberUuid + '\''
                + ", masterAddress=" + masterAddress + ", memberListVersion=" + memberListVersion + '}';
    }

}
