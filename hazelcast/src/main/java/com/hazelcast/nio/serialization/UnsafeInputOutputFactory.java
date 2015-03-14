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

package com.hazelcast.nio.serialization;

import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.BufferObjectDataOutput;

import java.nio.ByteOrder;

final class UnsafeInputOutputFactory implements InputOutputFactory {

    @Override
    public BufferObjectDataInput createInput(Data data, SerializationService service) {
        return new UnsafeObjectDataInput(data.getData(), DefaultData.DATA_OFFSET, service);
    }

    @Override
    public BufferObjectDataInput createInput(byte[] buffer, SerializationService service) {
        return new UnsafeObjectDataInput(buffer, service);
    }

    @Override
    public BufferObjectDataOutput createOutput(int size, SerializationService service) {
        return new UnsafeObjectDataOutput(size, service);
    }

    @Override
    public ByteOrder getByteOrder() {
        return ByteOrder.nativeOrder();
    }
}
