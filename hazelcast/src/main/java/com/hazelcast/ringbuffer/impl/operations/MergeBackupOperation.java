/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.ringbuffer.impl.RingbufferContainer;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.util.MapUtil;

import java.io.IOException;
import java.util.Map;

import static com.hazelcast.ringbuffer.impl.RingbufferDataSerializerHook.MERGE_BACKUP_OPERATION;

/**
 * Contains multiple backup entries for split-brain healing with a {@link com.hazelcast.spi.SplitBrainMergePolicy}.
 *
 * @since 3.10
 */
public class MergeBackupOperation extends AbstractRingBufferOperation implements BackupOperation {

    private Map<Long, Data> backupEntries;

    public MergeBackupOperation() {
    }

    MergeBackupOperation(String name, Map<Long, Data> backupEntries) {
        super(name);
        this.backupEntries = backupEntries;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void run() throws Exception {
        RingbufferContainer ringbuffer = getRingBufferContainer();

        for (Map.Entry<Long, Data> entry : backupEntries.entrySet()) {
            ringbuffer.set(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public int getId() {
        return MERGE_BACKUP_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(backupEntries.size());
        for (Map.Entry<Long, Data> entry : backupEntries.entrySet()) {
            out.writeLong(entry.getKey());
            out.writeData(entry.getValue());
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        backupEntries = MapUtil.createHashMap(size);
        for (int i = 0; i < size; i++) {
            backupEntries.put(in.readLong(), in.readData());
        }
    }
}
