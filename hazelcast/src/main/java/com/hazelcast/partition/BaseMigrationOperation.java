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

package com.hazelcast.partition;

import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.PartitionLevelOperation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class BaseMigrationOperation extends AbstractOperation
        implements PartitionLevelOperation, MigrationCycleOperation {

    protected MigrationInfo migrationInfo;

    protected transient boolean success = false;

    public BaseMigrationOperation() {
    }

    public BaseMigrationOperation(MigrationInfo migrationInfo) {
        this.migrationInfo = migrationInfo;
        setPartitionId(migrationInfo.getPartitionId())
                .setReplicaIndex(migrationInfo.getReplicaIndex());
    }

    public MigrationInfo getMigrationInfo() {
        return migrationInfo;
    }

    public MigrationType getMigrationType() {
        return migrationInfo.getMigrationType();
    }

    public boolean isMigration() {
        return getMigrationType() == MigrationType.MOVE
                || getMigrationType() == MigrationType.MOVE_COPY_BACK;
    }

    @Override
    public Object getResponse() {
        return success;
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    protected ILogger getLogger() {
        return getNodeEngine().getLogger(getClass().getName());
    }

    public void writeInternal(DataOutput out) throws IOException {
        migrationInfo.writeData(out);
    }

    public void readInternal(DataInput in) throws IOException {
        migrationInfo = new MigrationInfo();
        migrationInfo.readData(in);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(getClass().getName());
        sb.append("{partitionId=").append(getPartitionId());
        sb.append(", replicaIndex=").append(getReplicaIndex());
        sb.append(", migration=").append(migrationInfo);
        sb.append('}');
        return sb.toString();
    }
}
