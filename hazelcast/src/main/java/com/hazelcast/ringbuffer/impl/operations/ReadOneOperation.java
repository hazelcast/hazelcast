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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.ringbuffer.impl.RingbufferContainer;
import com.hazelcast.spi.impl.operationservice.BlockingOperation;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;

import java.io.IOException;

import static com.hazelcast.ringbuffer.impl.RingbufferDataSerializerHook.READ_ONE_OPERATION;

public class ReadOneOperation extends AbstractRingBufferOperation implements BlockingOperation, ReadonlyOperation {

    private long sequence;
    private Data result;

    public ReadOneOperation() {
    }

    public ReadOneOperation(String name, long sequence) {
        super(name);
        this.sequence = sequence;
    }

    @Override
    public void beforeRun() throws Exception {
        RingbufferContainer ringbuffer = getRingBufferContainerOrNull();
        if (ringbuffer != null) {
            ringbuffer.checkBlockableReadSequence(sequence);
        }
    }

    @Override
    public boolean shouldWait() {
        RingbufferContainer ringbuffer = getRingBufferContainerOrNull();
        if (ringbuffer == null) {
            return true;
        }
        if (ringbuffer.isTooLargeSequence(sequence) || ringbuffer.isStaleSequence(sequence)) {
            //no need to wait, let the operation continue and fail in beforeRun
            return false;
        }
        // the sequence is not readable
        return sequence == ringbuffer.tailSequence() + 1;
    }

    @Override
    public void run() throws Exception {
        RingbufferContainer ringbuffer = getRingBufferContainer();
        result = ringbuffer.readAsData(sequence);
    }

    @Override
    public WaitNotifyKey getWaitKey() {
        return getRingbufferWaitNotifyKey();
    }

    @Override
    public void afterRun() throws Exception {
        reportReliableTopicReceived(1);
    }

    @Override
    public void onWaitExpire() {
        //todo:
    }

    @Override
    public Data getResponse() {
        return result;
    }

    @Override
    public int getClassId() {
        return READ_ONE_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(sequence);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        sequence = in.readLong();
    }
}
