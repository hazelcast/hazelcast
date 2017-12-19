/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.ringbuffer.impl.operations;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.impl.MutatingOperation;

import java.io.IOException;

import static com.hazelcast.ringbuffer.impl.RingbufferDataSerializerHook.ADD_BACKUP_OPERATION;

/**
 * Backup operation for ring buffer {@link AddOperation}. Puts the item under the sequence ID that the master generated.
 */
public class AddBackupOperation extends AbstractRingBufferOperation implements BackupOperation, MutatingOperation {
    private long sequenceId;
    private Data item;

    public AddBackupOperation() {
    }

    public AddBackupOperation(String name, long sequenceId, Data item) {
        super(name);
        this.sequenceId = sequenceId;
        this.item = item;
    }

    @Override
    public void run() throws Exception {
        getRingBufferContainer().set(sequenceId, item);
    }

    @Override
    public int getId() {
        return ADD_BACKUP_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(sequenceId);
        out.writeData(item);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        sequenceId = in.readLong();
        item = in.readData();
    }
}
