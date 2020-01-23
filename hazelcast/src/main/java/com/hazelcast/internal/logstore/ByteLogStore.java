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

package com.hazelcast.internal.logstore;

import java.util.function.BinaryOperator;

public class ByteLogStore extends LogStore<Byte> {

    public ByteLogStore(LogStoreConfig config) {
        super(config);
    }

    public long putByte(byte v) {
        ensureCapacity(1);
        updateEdenTime();

        UNSAFE.putByte(eden.address + eden.position, v);
        long sequence = eden.position + eden.startSequence;
        eden.position++;
        eden.count++;
        return sequence;
    }

    public byte getByte(long sequence) {
        return UNSAFE.getByte(toAddress(sequence));
    }

    @Override
    public long putObject(Byte o) {
        return putByte(o);
    }

    @Override
    public Byte getObject(long sequence) {
        return getByte(sequence);
    }

    @Override
    public Byte reduce(BinaryOperator<Byte> accumulator) {
        throw new UnsupportedOperationException();
    }
}
