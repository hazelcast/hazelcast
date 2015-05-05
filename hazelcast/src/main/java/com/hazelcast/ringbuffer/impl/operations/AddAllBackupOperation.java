/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import java.io.IOException;

import static com.hazelcast.ringbuffer.impl.RingbufferDataSerializerHook.ADD_ALL_BACKUP_OPERATION;

public class AddAllBackupOperation extends AbstractRingBufferOperation implements BackupOperation {

    private Data[] items;

    public AddAllBackupOperation() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings({"EI_EXPOSE_REP" })
    public AddAllBackupOperation(String name, Data[] items) {
        super(name);
        this.items = items;
    }

    @Override
    public void run() throws Exception {
        RingbufferContainer ringbuffer = getRingBufferContainer();
        ringbuffer.addAll(items);
    }

    @Override
    public int getId() {
        return ADD_ALL_BACKUP_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(items.length);
        for (Data item : items) {
            out.writeData(item);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int length = in.readInt();
        items = new Data[length];
        for (int k = 0; k < items.length; k++) {
            items[k] = in.readData();
        }
    }
}

