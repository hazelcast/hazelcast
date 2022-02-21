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

import com.hazelcast.core.IFunction;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.ringbuffer.impl.ReadResultSetImpl;
import com.hazelcast.ringbuffer.impl.RingbufferContainer;
import com.hazelcast.spi.impl.operationservice.BlockingOperation;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;

import java.io.IOException;

import static com.hazelcast.ringbuffer.impl.RingbufferDataSerializerHook.READ_MANY_OPERATION;

public class ReadManyOperation<O> extends AbstractRingBufferOperation
        implements BlockingOperation, ReadonlyOperation {
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
    }

    @Override
    public boolean shouldWait() {
        RingbufferContainer ringbuffer = getRingBufferContainerOrNull();

        if (resultSet == null) {
            resultSet = new ReadResultSetImpl<>(minSize, maxSize, getNodeEngine().getSerializationService(), filter);
            sequence = startSequence;
        }

        if (ringbuffer == null) {
            return minSize > 0;
        }

        sequence = ringbuffer.clampReadSequenceToBounds(sequence);

        if (minSize == 0) {
            if (sequence < ringbuffer.tailSequence() + 1) {
                readMany(ringbuffer);
            }

            return false;
        }

        if (resultSet.isMinSizeReached()) {
            // enough items have been read, we are done.
            return false;
        }

        if (sequence == ringbuffer.tailSequence() + 1) {
            // the sequence is not readable
            return true;
        }
        readMany(ringbuffer);
        return !resultSet.isMinSizeReached();
    }

    private void readMany(RingbufferContainer ringbuffer) {
        sequence = ringbuffer.readMany(sequence, resultSet);
        resultSet.setNextSequenceToReadFrom(sequence);
    }

    @Override
    public void run() throws Exception {
        // no-op; we already did the work in the shouldWait method.
    }

    @Override
    public void afterRun() throws Exception {
        reportReliableTopicReceived(resultSet.size());
    }

    @Override
    public Object getResponse() {
        return resultSet;
    }

    @Override
    public WaitNotifyKey getWaitKey() {
        return getRingbufferWaitNotifyKey();
    }

    @Override
    public void onWaitExpire() {
        //todo:
    }

    @Override
    public int getClassId() {
        return READ_MANY_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(startSequence);
        out.writeInt(minSize);
        out.writeInt(maxSize);
        out.writeObject(filter);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        startSequence = in.readLong();
        minSize = in.readInt();
        maxSize = in.readInt();
        filter = in.readObject();
    }
}
