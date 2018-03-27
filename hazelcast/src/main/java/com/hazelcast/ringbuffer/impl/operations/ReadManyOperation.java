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

import com.hazelcast.core.IFunction;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.ringbuffer.impl.ReadResultSetImpl;
import com.hazelcast.ringbuffer.impl.RingbufferContainer;
import com.hazelcast.spi.BlockingOperation;
import com.hazelcast.spi.ReadonlyOperation;
import com.hazelcast.spi.WaitNotifyKey;

import java.io.IOException;

import static com.hazelcast.internal.cluster.Versions.V3_10;
import static com.hazelcast.ringbuffer.impl.RingbufferDataSerializerHook.READ_MANY_OPERATION;

public class ReadManyOperation<O> extends AbstractRingBufferOperation
        implements BlockingOperation, ReadonlyOperation, Versioned {
    transient long sequence;

    private int minSize;
    private int maxSize;
    private long startSequence;
    private IFunction<O, Boolean> filter;

    private transient ReadResultSetImpl<O, O> resultSet;

    public ReadManyOperation() {
    }

    public ReadManyOperation(String name, long startSequence, int minSize, int maxSize, IFunction<O, Boolean> filter) {
        super(name);
        this.minSize = minSize;
        this.maxSize = maxSize;
        this.startSequence = startSequence;
        this.filter = filter;
    }

    @Override
    public void beforeRun() {
        RingbufferContainer ringbuffer = getRingBufferContainer();
        ringbuffer.checkBlockableReadSequence(startSequence);
    }

    @Override
    public boolean shouldWait() {
        if (resultSet == null) {
            resultSet = new ReadResultSetImpl<O, O>(minSize, maxSize, getNodeEngine().getSerializationService(), filter);
            sequence = startSequence;
        }

        RingbufferContainer ringbuffer = getRingBufferContainer();
        if (minSize == 0) {
            if (!ringbuffer.shouldWait(sequence)) {
                sequence = ringbuffer.readMany(sequence, resultSet);
            }

            return false;
        }

        if (resultSet.isMinSizeReached()) {
            // enough items have been read, we are done.
            return false;
        }

        if (ringbuffer.shouldWait(sequence)) {
            // the sequence is not readable
            return true;
        }

        sequence = ringbuffer.readMany(sequence, resultSet);
        return !resultSet.isMinSizeReached();
    }

    @Override
    public void run() throws Exception {
        // no-op; we already did the work in the shouldWait method.
    }

    @Override
    public Object getResponse() {
        return resultSet;
    }

    @Override
    public WaitNotifyKey getWaitKey() {
        RingbufferContainer ringbuffer = getRingBufferContainer();
        return ringbuffer.getRingEmptyWaitNotifyKey();
    }

    @Override
    public void onWaitExpire() {
        //todo:
    }

    @Override
    public int getId() {
        return READ_MANY_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(startSequence);
        out.writeInt(minSize);
        out.writeInt(maxSize);
        out.writeObject(filter);
        // RU_COMPAT_3_9
        if (out.getVersion().isUnknownOrLessThan(V3_10)) {
            out.writeBoolean(false);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        startSequence = in.readLong();
        minSize = in.readInt();
        maxSize = in.readInt();
        filter = in.readObject();
        // RU_COMPAT_3_9
        if (in.getVersion().isUnknownOrLessThan(V3_10)) {
            // consume an unused boolean
            in.readBoolean();
        }
    }
}
