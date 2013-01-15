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

package com.hazelcast.partition;

import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

/**
 * will be moved to com.hazelcast.core package
 */

public class MigrationEvent implements DataSerializable {
    private int partitionId;
    private Member oldOwner;
    private Member newOwner;
    private MigrationStatus status;

    public MigrationEvent() {
    }

    public MigrationEvent(int partitionId, Member oldOwner, Member newOwner, MigrationStatus status) {
        this.partitionId = partitionId;
        this.oldOwner = oldOwner;
        this.newOwner = newOwner;
        this.status = status;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public Member getOldOwner() {
        return oldOwner;
    }

    public Member getNewOwner() {
        return newOwner;
    }

    public MigrationStatus getStatus() {
        return status;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(partitionId);
        oldOwner.writeData(out);
        newOwner.writeData(out);
        MigrationStatus.writeTo(status, out);
    }

    public void readData(ObjectDataInput in) throws IOException {
        partitionId = in.readInt();
        oldOwner = new MemberImpl();
        oldOwner.readData(in);
        newOwner = new MemberImpl();
        newOwner.readData(in);
        status = MigrationStatus.readFrom(in);
    }

    @Override
    public String toString() {
        return "MigrationEvent{" +
                "partitionId=" + partitionId +
                ", oldOwner=" + oldOwner +
                ", newOwner=" + newOwner +
                '}';
    }
}
