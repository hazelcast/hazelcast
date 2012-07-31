/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

import com.hazelcast.impl.spi.AbstractOperation;
import com.hazelcast.impl.spi.NoReply;
import com.hazelcast.impl.spi.NonBlockingOperation;
import com.hazelcast.nio.Address;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MigrationNotification extends AbstractOperation implements NoReply, NonBlockingOperation {
    MigratingPartition migratingPartition;
    boolean started;

    public MigrationNotification() {
    }

    public MigrationNotification(boolean started, MigratingPartition migratingPartition) {
        this.started = started;
        this.migratingPartition = migratingPartition;
    }

    public void run() {
        Address from = migratingPartition.getFromAddress();
        Address to = migratingPartition.getToAddress();
        int partitionId = migratingPartition.getPartitionId();
        getOperationContext().getNodeService().getNode()
                .partitionManager.fireMigrationEvent(started, partitionId, from, to);
    }

    @Override
    public void readData(DataInput in) throws IOException {
        super.readData(in);
        migratingPartition = new MigrationRequestOperation();
        migratingPartition.readData(in);
        started = in.readBoolean();
    }

    @Override
    public void writeData(DataOutput out) throws IOException {
        super.writeData(out);
        migratingPartition.writeData(out);
        out.writeBoolean(started);
    }
}