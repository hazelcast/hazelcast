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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InputOutputFactory;
import com.hazelcast.internal.serialization.InternalSerializationService;

import java.nio.ByteOrder;

final class ByteArrayInputOutputFactory implements InputOutputFactory {

    private final ByteOrder byteOrder;

    ByteArrayInputOutputFactory(ByteOrder byteOrder) {
        this.byteOrder = byteOrder;
    }

    @Override
    public BufferObjectDataInput createInput(Data data,
                                             InternalSerializationService service,
                                             boolean isCompatibility) {
        return new ByteArrayObjectDataInput(data.toByteArray(), HeapData.DATA_OFFSET, service, byteOrder, isCompatibility);
    }

    @Override
    public BufferObjectDataInput createInput(byte[] buffer,
                                             InternalSerializationService service,
                                             boolean isCompatibility) {
        return new ByteArrayObjectDataInput(buffer, service, byteOrder, isCompatibility);
    }

    @Override
    public BufferObjectDataInput createInput(byte[] buffer,
                                             int offset,
                                             InternalSerializationService service,
                                             boolean isCompatibility) {
        return new ByteArrayObjectDataInput(buffer, offset, service, byteOrder, isCompatibility);
    }

    @Override
    public BufferObjectDataOutput createOutput(int size, InternalSerializationService service) {
        return new ByteArrayObjectDataOutput(size, service, byteOrder);
    }

    @Override
    public BufferObjectDataOutput createOutput(int initialSize, int firstGrowthSize, InternalSerializationService service) {
        return new ByteArrayObjectDataOutput(initialSize, firstGrowthSize, service, byteOrder);
    }

    @Override
    public ByteOrder getByteOrder() {
        return byteOrder;
    }
}
