/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.serialization.InputOutputFactory;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.serialization.Data;

import java.nio.ByteOrder;

final class UnsafeInputOutputFactory implements InputOutputFactory {

    @Override
    public BufferObjectDataInput createInput(Data data, InternalSerializationService service) {
        return new UnsafeObjectDataInput(data.toByteArray(), HeapData.DATA_OFFSET, service);
    }

    @Override
    public BufferObjectDataInput createInput(byte[] buffer, InternalSerializationService service) {
        return new UnsafeObjectDataInput(buffer, service);
    }

    @Override
    public BufferObjectDataInput createInput(byte[] buffer, int offset, InternalSerializationService service) {
        return new UnsafeObjectDataInput(buffer, offset, service);
    }

    @Override
    public BufferObjectDataOutput createOutput(int size, InternalSerializationService service) {
        return new UnsafeObjectDataOutput(size, service);
    }

    @Override
    public ByteOrder getByteOrder() {
        return ByteOrder.nativeOrder();
    }
}
