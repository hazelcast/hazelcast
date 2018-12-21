/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition;

import com.hazelcast.core.Member;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * PartitionReplica represents owner of a partition replica
 * in the partition table.
 * <p>
 * A PartitionReplica is identical to a {@link Member}
 * which has the same {@code address} and {@code uuid}.
 * <p>
 * Existing {@link Member} interface could be used instead of introducing
 * PartitionReplica class but {@link Member} has many additional attributes
 * and functionality which adds complexity to maintenance of partition table.
 *
 * @see InternalPartition
 * @see Member
 * @since 3.12
 */
public final class PartitionReplica implements IdentifiedDataSerializable {

    // RU_COMPAT_3_11
    public static final String UNKNOWN_UID = "<unknown-uuid>";

    private Address address;

    private String uuid;

    public PartitionReplica() {
    }

    public PartitionReplica(Address address, String uuid) {
        assert address != null;
        assert uuid != null;
        this.address = address;
        this.uuid = uuid;
    }

    public Address address() {
        return address;
    }

    public String uuid() {
        return uuid;
    }

    public boolean isIdentical(Member member) {
        return address.equals(member.getAddress()) && uuid.equals(member.getUuid());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PartitionReplica)) {
            return false;
        }

        PartitionReplica replica = (PartitionReplica) o;

        if (!address.equals(replica.address)) {
            return false;
        }
        return uuid.equals(replica.uuid);
    }

    @Override
    public int hashCode() {
        int result = address.hashCode();
        result = 31 * result + uuid.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "[" + address.getHost() + "]" + ":" + address.getPort() + " - " + uuid;
    }

    @Override
    public int getFactoryId() {
        return PartitionDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return PartitionDataSerializerHook.PARTITION_REPLICA;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(address);
        out.writeUTF(uuid);
    }

    public void readData(ObjectDataInput in) throws IOException {
        address = in.readObject();
        uuid = in.readUTF();
    }

    public static PartitionReplica from(Member member) {
        return new PartitionReplica(member.getAddress(), member.getUuid());
    }

    public static PartitionReplica[] from(Member[] members) {
        PartitionReplica[] replicas = new PartitionReplica[members.length];
        for (int i = 0; i < members.length; i++) {
            replicas[i] = from(members[i]);
        }
        return replicas;
    }
}
