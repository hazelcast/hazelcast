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

package com.hazelcast.cp.internal;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Implementation of {@link CPGroupId}.
 */
public final class RaftGroupId implements CPGroupId, IdentifiedDataSerializable, Serializable {

    private static final long serialVersionUID = -2381010126931378167L;

    private String name;
    private long seed;
    private long groupId;

    public RaftGroupId() {
    }

    public RaftGroupId(String name, long seed, long groupId) {
        assert name != null;
        this.name = name;
        this.seed = seed;
        this.groupId = groupId;
    }

    @Override
    public String getName() {
        return name;
    }

    public long getSeed() {
        return seed;
    }

    @Override
    public long getId() {
        return groupId;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeLong(seed);
        out.writeLong(groupId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readString();
        seed = in.readLong();
        groupId = in.readLong();
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeUTF(name);
        out.writeLong(seed);
        out.writeLong(groupId);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        name = in.readUTF();
        seed = in.readLong();
        groupId = in.readLong();
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftServiceDataSerializerHook.GROUP_ID;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RaftGroupId that = (RaftGroupId) o;

        if (seed != that.seed) {
            return false;
        }
        if (groupId != that.groupId) {
            return false;
        }
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + (int) (seed ^ (seed >>> 32));
        result = 31 * result + (int) (groupId ^ (groupId >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "CPGroupId{" + "name='" + name + '\'' + ", seed=" + seed + ", groupId=" + groupId + '}';
    }
}
