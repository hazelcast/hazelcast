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
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.ringbuffer.impl.RingbufferContainer;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;
import com.hazelcast.spi.impl.operationservice.Notifier;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;

import static com.hazelcast.ringbuffer.OverflowPolicy.FAIL;
import static com.hazelcast.ringbuffer.impl.RingbufferDataSerializerHook.ADD_ALL_OPERATION;

/**
 * Adds a batch of items to the ring buffer. The master node will add the items into the ring buffer, generating sequence IDs.
 * The backup operation will put the items under the generated sequence IDs that the master generated. This is to avoid
 * differences in ring buffer data structures.
 */
public class AddAllOperation extends AbstractRingBufferOperation
        implements Notifier, BackupAwareOperation, MutatingOperation {

    private OverflowPolicy overflowPolicy;
    private Data[] items;
    private long lastSequence;

    public AddAllOperation() {
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public AddAllOperation(String name, Data[] items, OverflowPolicy overflowPolicy) {
        super(name);
        this.items = items;
        this.overflowPolicy = overflowPolicy;
    }

    @Override
    public void run() throws Exception {
        final RingbufferContainer ringbuffer = getRingBufferContainer();

        if (overflowPolicy == FAIL) {
            if (ringbuffer.remainingCapacity() < items.length) {
                lastSequence = -1;
                return;
            }
        }

        lastSequence = ringbuffer.addAll(items);
    }

    @Override
    public void afterRun() throws Exception {
        reportReliableTopicPublish(items.length);
    }

    @Override
    public Object getResponse() {
        return lastSequence;
    }

    @Override
    public boolean shouldNotify() {
        return lastSequence != -1;
    }

    @Override
    public WaitNotifyKey getNotifiedKey() {
        return getRingbufferWaitNotifyKey();
    }

    @Override
    public boolean shouldBackup() {
        return lastSequence != -1;
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
        return new AddAllBackupOperation(name, lastSequence, items);
    }

    @Override
    public int getClassId() {
        return ADD_ALL_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        out.writeInt(overflowPolicy.getId());

        out.writeInt(items.length);
        for (Data item : items) {
            IOUtil.writeData(out, item);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        overflowPolicy = OverflowPolicy.getById(in.readInt());

        int length = in.readInt();
        items = new Data[length];
        for (int k = 0; k < items.length; k++) {
            items[k] = IOUtil.readData(in);
        }
    }
}

