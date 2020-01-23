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

import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;

public class IntLogStore extends LogStore<Integer> {

    public IntLogStore(LogStoreConfig config) {
        super(config);
    }

    public long putInt(int v) {
        ensureCapacity(INT_SIZE_IN_BYTES);
        updateEdenTime();

        UNSAFE.putInt(eden.address + eden.position, v);
        long sequence = eden.position + eden.startSequence;
        eden.position += INT_SIZE_IN_BYTES;
        eden.count++;
        return sequence;
    }

    public int getInt(long sequence) {
        return UNSAFE.getInt(toAddress(sequence));
    }

    @Override
    public long putObject(Integer o) {
        return putInt(o.intValue());
    }

    @Override
    public Integer getObject(long sequence) {
        return getInt(sequence);
    }

    @Override
    public Integer reduce(BinaryOperator<Integer> accumulator) {
        throw new UnsupportedOperationException();
    }
}
