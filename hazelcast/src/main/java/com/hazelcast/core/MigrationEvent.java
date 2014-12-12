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

package com.hazelcast.core;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * An event fired when a partition migration starts, completes or fails.
 *
 * @see Partition
 * @see PartitionService
 * @see MigrationListener
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

    /**
     * Returns the id of the partition which is (or is being) migrated
     *
     * @return the id of the partition which is (or is being) migrated
     */
    public int getPartitionId() {
        return partitionId;
    }

    /**
     * Returns the old owner of the migrating partition
     *
     * @return the old owner of the migrating partition
     */
    public Member getOldOwner() {
        return oldOwner;
    }

    /**
     * Returns the new owner of the migrating partition
     *
     * @return the new owner of the migrating partition
     */
    public Member getNewOwner() {
        return newOwner;
    }

    /**
     * Returns the status of the migration: started, completed or failed
     *
     * @return the migration status: started, completed or failed
     */
    public MigrationStatus getStatus() {
        return status;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(partitionId);
        out.writeObject(oldOwner);
        out.writeObject(newOwner);
        MigrationStatus.writeTo(status, out);
    }

    public void readData(ObjectDataInput in) throws IOException {
        partitionId = in.readInt();
        oldOwner = in.readObject();
        newOwner = in.readObject();
        status = MigrationStatus.readFrom(in);
    }

    /**
     * Migration status: Started, completed or failed
     */
    public static enum MigrationStatus {
        /**
         * Migration has been started
         */
        STARTED(0),

        /**
         * Migration has been completed
         */
        COMPLETED(1),

        /**
         * Migration has failed
         */
        FAILED(-1);

        private final byte code;

        private MigrationStatus(final int code) {
            this.code = (byte) code;
        }

        public static void writeTo(MigrationStatus status, DataOutput out) throws IOException {
            out.writeByte(status.code);
        }

        public static MigrationStatus readFrom(DataInput in) throws IOException {
            final byte code = in.readByte();
            switch (code) {
                case 0:
                    return STARTED;
                case 1:
                    return COMPLETED;
                case -1:
                    return FAILED;
                default:
            }
            return null;
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MigrationEvent{");
        sb.append("partitionId=").append(partitionId);
        sb.append(", status=").append(status);
        sb.append(", oldOwner=").append(oldOwner);
        sb.append(", newOwner=").append(newOwner);
        sb.append('}');
        return sb.toString();
    }
}
