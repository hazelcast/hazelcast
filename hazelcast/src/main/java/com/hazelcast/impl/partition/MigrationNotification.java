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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MigrationNotification extends AbstractOperation implements NoReply, NonBlockingOperation {
    MigratingPartition migratingPartition;
    MigrationStatus status;

    public MigrationNotification() {
    }

    public MigrationNotification(MigrationStatus status, MigratingPartition migratingPartition) {
        this.status = status;
        this.migratingPartition = migratingPartition;
    }

    public void run() {
        getNodeService().getNode()
                .partitionManager.fireMigrationEvent(status, migratingPartition);
    }

    @Override
    public void readInternal(DataInput in) throws IOException {
        super.readInternal(in);
        migratingPartition = new MigratingPartition();
        migratingPartition.readData(in);
        status = MigrationStatus.get(in.readByte());
    }

    @Override
    public void writeInternal(DataOutput out) throws IOException {
        super.writeInternal(out);
        migratingPartition.writeData(out);
        out.writeByte(status.getCode());
    }
}