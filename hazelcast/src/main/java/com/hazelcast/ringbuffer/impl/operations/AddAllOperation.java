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
import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.ringbuffer.impl.RingbufferContainer;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Notifier;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitNotifyKey;

import java.io.IOException;

import static com.hazelcast.ringbuffer.OverflowPolicy.FAIL;
import static com.hazelcast.ringbuffer.impl.RingbufferDataSerializerHook.ADD_ALL_OPERATION;

public class AddAllOperation extends AbstractRingBufferOperation
        implements Notifier, BackupAwareOperation {

    private OverflowPolicy overflowPolicy;
    private Data[] items;
    private long resultSequence;

    public AddAllOperation() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings({"EI_EXPOSE_REP" })
    public AddAllOperation(String name, Data[] items, OverflowPolicy overflowPolicy) {
        super(name);
        this.items = items;
        this.overflowPolicy = overflowPolicy;
    }

    @Override
    public void run() throws Exception {
        RingbufferContainer ringbuffer = getRingBufferContainer();

        if (overflowPolicy == FAIL) {
            if (ringbuffer.remainingCapacity() < items.length) {
                resultSequence = -1;
                return;
            }
        }

        resultSequence = ringbuffer.addAll(items);
    }

    @Override
    public Object getResponse() {
        return resultSequence;
    }

    @Override
    public boolean shouldNotify() {
        return resultSequence != -1;
    }

    @Override
    public WaitNotifyKey getNotifiedKey() {
        RingbufferContainer ringbuffer = getRingBufferContainer();
        return ringbuffer.getRingEmptyWaitNotifyKey();
    }

    @Override
    public boolean shouldBackup() {
        return resultSequence != -1;
    }

    @Override
    public int getSyncBackupCount() {
        RingbufferContainer ringbuffer = getRingBufferContainer();
        return ringbuffer.getConfig().getBackupCount();
    }

    @Override
    public int getAsyncBackupCount() {
        RingbufferContainer ringbuffer = getRingBufferContainer();
        return ringbuffer.getConfig().getAsyncBackupCount();
    }

    @Override
    public Operation getBackupOperation() {
        return new AddAllBackupOperation(name, items);
    }

    @Override
    public int getId() {
        return ADD_ALL_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        out.writeInt(overflowPolicy.getId());

        out.writeInt(items.length);
        for (Data item : items) {
            out.writeData(item);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        overflowPolicy = OverflowPolicy.getById(in.readInt());

        int length = in.readInt();
        items = new Data[length];
        for (int k = 0; k < items.length; k++) {
            items[k] = in.readData();
        }
    }
}

