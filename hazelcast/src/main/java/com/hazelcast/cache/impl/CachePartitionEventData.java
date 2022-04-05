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

package com.hazelcast.cache.impl;

import com.hazelcast.cache.CacheEventType;
import com.hazelcast.cluster.Member;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.BinaryInterface;

import java.io.IOException;

@BinaryInterface
public class CachePartitionEventData extends CacheEventDataImpl implements CacheEventData {

    private int partitionId;
    private Member member;

    public CachePartitionEventData() {
    }

    public CachePartitionEventData(String name, int partitionId, Member member) {
        super(name, CacheEventType.PARTITION_LOST, null, null, null, false);
        this.partitionId = partitionId;
        this.member = member;
    }

    public Member getMember() {
        return member;
    }

    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        super.writeData(out);
        out.writeInt(partitionId);
        out.writeObject(member);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        super.readData(in);
        partitionId = in.readInt();
        member = in.readObject();
    }

    @Override
    public String toString() {
        return "CachePartitionEventData{"
                + super.toString()
                + ", partitionId=" + partitionId
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CachePartitionEventData)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        CachePartitionEventData that = (CachePartitionEventData) o;
        if (partitionId != that.partitionId) {
            return false;
        }
        if (member != null ? !member.equals(that.member) : that.member != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + partitionId;
        result = 31 * result + (member != null ? member.hashCode() : 0);
        return result;
    }

    @Override
    public int getClassId() {
        return CacheDataSerializerHook.CACHE_PARTITION_EVENT_DATA;
    }
}
