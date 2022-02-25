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

package com.hazelcast.jet.impl.serialization;

import com.hazelcast.internal.nio.Bits;

import java.nio.ByteOrder;

public class PlainMemoryReader implements MemoryReader {

    private final boolean bigEndian;

    public PlainMemoryReader(ByteOrder byteOrder) {
        this.bigEndian = byteOrder == ByteOrder.BIG_ENDIAN;
    }

    @Override
    public int readInt(byte[] bytes, int offset) {
        return Bits.readInt(bytes, offset, bigEndian);
    }

    @Override
    public long readLong(byte[] bytes, int offset) {
        return Bits.readLong(bytes, offset, bigEndian);
    }
}
