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

package com.hazelcast.internal.serialization;

import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.BufferObjectDataOutput;

import java.nio.ByteOrder;

public interface InputOutputFactory {

    BufferObjectDataInput createInput(Data data,
                                      InternalSerializationService service,
                                      boolean isCompatibility);

    BufferObjectDataInput createInput(byte[] buffer,
                                      InternalSerializationService service,
                                      boolean isCompatibility);

    BufferObjectDataInput createInput(byte[] buffer,
                                      int offset,
                                      InternalSerializationService service,
                                      boolean isCompatibility);

    BufferObjectDataOutput createOutput(int size, InternalSerializationService service);

    /**
     * Creating new {@link BufferObjectDataOutput}. The size of the internal buffer is set to initialSize at construction.
     * When the buffer is too small to accept new bytes the buffer will grow to at least firstGrowthSize.
     */
    BufferObjectDataOutput createOutput(int initialSize, int firstGrowthSize, InternalSerializationService service);

    ByteOrder getByteOrder();
}
