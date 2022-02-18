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

package com.hazelcast.ringbuffer.impl.operations;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.ringbuffer.impl.RingbufferContainer;
import com.hazelcast.spi.impl.operationservice.BackupOperation;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;

import static com.hazelcast.ringbuffer.impl.RingbufferDataSerializerHook.ADD_ALL_BACKUP_OPERATION;

/**
 * Backup operation for ring buffer {@link AddAllOperation}. Puts the items under the sequence IDs that the master generated.
 */
public class AddAllBackupOperation extends AbstractRingBufferOperation implements BackupOperation {
    private long lastSequenceId;
    private Data[] items;

    public AddAllBackupOperation() {
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public AddAllBackupOperation(String name, long lastSequenceId, Data[] items) {
        super(name);
        this.items = items;
        this.lastSequenceId = lastSequenceId;
    }

    @Override
    public void run() throws Exception {
        final RingbufferContainer ringbuffer = getRingBufferContainer();
        final long firstSequenceId = lastSequenceId - items.length + 1;

        for (int i = 0; i < items.length; i++) {
            ringbuffer.set(firstSequenceId + i, items[i]);
        }
    }

    @Override
    public int getClassId() {
        return ADD_ALL_BACKUP_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(lastSequenceId);
        out.writeInt(items.length);
        for (Data item : items) {
            IOUtil.writeData(out, item);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        lastSequenceId = in.readLong();
        int length = in.readInt();
        items = new Data[length];
        for (int k = 0; k < items.length; k++) {
            items[k] = IOUtil.readData(in);
        }
    }
}

