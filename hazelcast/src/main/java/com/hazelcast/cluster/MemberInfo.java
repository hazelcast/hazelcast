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

package com.hazelcast.cluster;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.MemberAttributes;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class MemberInfo implements DataSerializable {
    Address address = null;
    String uuid;
    MemberAttributes memberAttributes;

    public MemberInfo() {
    }

    public MemberInfo(Address address) {
        super();
        this.address = address;
    }
//
//    public MemberInfo(Address address, String uuid) {
//        super();
//        this.address = address;
//        this.uuid = uuid;
//    }


    public MemberInfo(Address address, MemberAttributes memberAttributes) {
        super();
        this.address = address;
        this.memberAttributes = memberAttributes;
    }

    public MemberInfo(Address address, String uuid, MemberAttributes memberAttributes) {
        super();
        this.address = address;
        this.uuid = uuid;
        this.memberAttributes = memberAttributes;
    }

    public void readData(ObjectDataInput in) throws IOException {
        address = new Address();
        address.readData(in);
        if (in.readBoolean()) {
            uuid = in.readUTF();
        }
        if (in.readBoolean()) {
            memberAttributes = new MemberAttributes();
            memberAttributes.readData(in);
        }
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        address.writeData(out);
        boolean hasUuid = uuid != null;
        out.writeBoolean(hasUuid);
        if (hasUuid) {
            out.writeUTF(uuid);
        }
        boolean hasMemberAttributes = memberAttributes != null;
        out.writeBoolean(hasMemberAttributes);
        if (hasMemberAttributes) {
            memberAttributes.writeData(out);
        }
    }

    public Address getAddress() {
        return address;
    }

    public MemberAttributes getMemberAttributes() {
        return memberAttributes;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((address == null) ? 0 : address.hashCode());
        result = prime * result + ((memberAttributes == null) ? 0 : memberAttributes.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        MemberInfo other = (MemberInfo) obj;
        if (address == null) {
            if (other.address != null)
                return false;
        } else if (!address.equals(other.address))
            return false;
        if (memberAttributes == null) {
            if (other.memberAttributes != null)
                return false;
        } else if (!memberAttributes.equals(other.memberAttributes)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "MemberInfo{" +
                "address=" + address +
                '}';
    }
}
