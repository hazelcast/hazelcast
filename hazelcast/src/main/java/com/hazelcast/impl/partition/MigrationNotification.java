/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.impl.partition;

import com.hazelcast.cluster.AbstractRemotelyProcessable;
import com.hazelcast.nio.Address;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MigrationNotification extends AbstractRemotelyProcessable {
    MigrationRequestTask migrationRequestTask;
    MigrationStatus status;

    public MigrationNotification() {
    }

    public MigrationNotification(MigrationStatus status, MigrationRequestTask migrationRequestTask) {
        this.status = status;
        this.migrationRequestTask = migrationRequestTask;
    }

    public void process() {
        Address from = migrationRequestTask.getFromAddress();
        Address to = migrationRequestTask.getToAddress();
        int partitionId = migrationRequestTask.getPartitionId();
        node.concurrentMapManager.getPartitionManager().fireMigrationEvent(status, partitionId, from, to);
    }

    @Override
    public void readData(DataInput in) throws IOException {
        super.readData(in);
        migrationRequestTask = new MigrationRequestTask();
        migrationRequestTask.readData(in);
        status = MigrationStatus.get(in.readByte());
    }

    @Override
    public void writeData(DataOutput out) throws IOException {
        super.writeData(out);
        migrationRequestTask.writeData(out);
        out.writeByte(status.getCode());
    }
}