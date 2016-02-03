/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import static com.hazelcast.ringbuffer.impl.RingbufferDataSerializerHook.ADD_OPERATION;

public class AddOperation extends AbstractRingBufferOperation
        implements Notifier, BackupAwareOperation {

    private Data item;
    private long resultSequence;
    private OverflowPolicy overflowPolicy;

    public AddOperation() {
    }

    public AddOperation(String name, Data item, OverflowPolicy overflowPolicy) {
        super(name);
        this.item = item;
        this.overflowPolicy = overflowPolicy;
    }

    @Override
    public void run() throws Exception {
        RingbufferContainer ringbuffer = getRingBufferContainer();

        //todo: move into ringbuffer.
        if (overflowPolicy == FAIL) {
            if (ringbuffer.remainingCapacity() < 1) {
                resultSequence = -1;
                return;
            }
        }

        resultSequence = ringbuffer.add(item);
    }

    @Override
    public WaitNotifyKey getNotifiedKey() {
        RingbufferContainer ringbuffer = getRingBufferContainer();
        return ringbuffer.getRingEmptyWaitNotifyKey();
    }

    @Override
    public boolean shouldNotify() {
        return resultSequence != -1;
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
        return new AddBackupOperation(name, item);
    }

    @Override
    public Long getResponse() {
        return resultSequence;
    }

    @Override
    public int getId() {
        return ADD_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeData(item);
        out.writeInt(overflowPolicy.getId());
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        item = in.readData();
        overflowPolicy = OverflowPolicy.getById(in.readInt());
    }
}
