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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.UUID;

public class MembersViewResponse implements IdentifiedDataSerializable {

    private Address memberAddress;

    private UUID memberUuid;

    private MembersView membersView;

    public MembersViewResponse() {
    }

    public MembersViewResponse(Address memberAddress, UUID memberUuid, MembersView membersView) {
        this.memberAddress = memberAddress;
        this.memberUuid = memberUuid;
        this.membersView = membersView;
    }


    public Address getMemberAddress() {
        return memberAddress;
    }

    public UUID getMemberUuid() {
        return memberUuid;
    }

    public MembersView getMembersView() {
        return membersView;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(memberAddress);
        UUIDSerializationUtil.writeUUID(out, memberUuid);
        out.writeObject(membersView);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        memberAddress = in.readObject();
        memberUuid = UUIDSerializationUtil.readUUID(in);
        membersView = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return ClusterDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ClusterDataSerializerHook.MEMBERS_VIEW_RESPONSE;
    }
}
