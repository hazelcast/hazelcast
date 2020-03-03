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

import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.serialization.InternalSerializationService;

import java.nio.ByteOrder;

// remove when `InternalSerializationService.createObjectDataInput(byte[] data, int offset)` is there...
// https://github.com/hazelcast/hazelcast/pull/16701
@Deprecated
public interface CustomInputOutputFactory {

    BufferObjectDataInput createInput(byte[] bytes, int offset);

    static CustomInputOutputFactory from(InternalSerializationService serializationService) {
        if (((AbstractSerializationService) serializationService).inputOutputFactory instanceof
                com.hazelcast.internal.serialization.impl.ByteArrayInputOutputFactory) {
            return new ByteArrayInputOutputFactory(serializationService);
        } else {
            return new UnsafeInputOutputFactory(serializationService);
        }
    }

    class ByteArrayInputOutputFactory implements CustomInputOutputFactory {

        private final ByteOrder byteOrder;
        private final InternalSerializationService serializationService;

        public ByteArrayInputOutputFactory(InternalSerializationService serializationService) {
            this.byteOrder = serializationService.getByteOrder();
            this.serializationService = serializationService;
        }

        @Override
        public BufferObjectDataInput createInput(byte[] bytes, int offset) {
            return new ByteArrayObjectDataInput(bytes, offset, serializationService, byteOrder);
        }
    }

    class UnsafeInputOutputFactory implements CustomInputOutputFactory {

        private final InternalSerializationService serializationService;

        public UnsafeInputOutputFactory(InternalSerializationService serializationService) {
            this.serializationService = serializationService;
        }

        @Override
        public BufferObjectDataInput createInput(byte[] bytes, int offset) {
            return new UnsafeObjectDataInput(bytes, offset, serializationService);
        }
    }
}
