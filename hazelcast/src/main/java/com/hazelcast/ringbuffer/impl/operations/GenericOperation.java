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
import com.hazelcast.ringbuffer.impl.RingbufferContainer;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;

import java.io.IOException;

import static com.hazelcast.ringbuffer.impl.RingbufferDataSerializerHook.GENERIC_OPERATION;

/**
 * Ring buffer operations which don't need to send any parameters and which can be determined by only sending the operation
 * type which is a byte of information. Typically these are read operations and/or getters.
 */
public class GenericOperation extends AbstractRingBufferOperation implements ReadonlyOperation {

    public static final byte OPERATION_SIZE = 0;
    public static final byte OPERATION_TAIL = 1;
    public static final byte OPERATION_HEAD = 2;
    public static final byte OPERATION_REMAINING_CAPACITY = 3;
    public static final byte OPERATION_CAPACITY = 4;

    byte operation;
    private transient long result;

    public GenericOperation() {
    }

    public GenericOperation(String name, byte operation) {
        super(name);
        this.operation = operation;
    }

    @Override
    public void run() throws Exception {
        RingbufferContainer ringbuffer = getRingBufferContainer();
        switch (operation) {
            case OPERATION_SIZE:
                result = ringbuffer.size();
                break;
            case OPERATION_HEAD:
                result = ringbuffer.headSequence();
                break;
            case OPERATION_TAIL:
                result = ringbuffer.tailSequence();
                break;
            case OPERATION_REMAINING_CAPACITY:
                result = ringbuffer.remainingCapacity();
                break;
            case OPERATION_CAPACITY:
                result = ringbuffer.getCapacity();
                break;
            default:
                throw new IllegalStateException("Unrecognized operation:" + operation);
        }
    }

    @Override
    public Long getResponse() {
        return result;
    }

    @Override
    public int getClassId() {
        return GENERIC_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeByte(operation);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        operation = in.readByte();
    }
}
