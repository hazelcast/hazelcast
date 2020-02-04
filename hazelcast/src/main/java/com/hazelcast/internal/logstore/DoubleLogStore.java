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

import static com.hazelcast.internal.nio.Bits.DOUBLE_SIZE_IN_BYTES;

public class DoubleLogStore extends LogStore<Double> {

    public DoubleLogStore(LogStoreConfig config) {
        super(config);
    }

    public long putDouble(double v) {
        ensureCapacity(DOUBLE_SIZE_IN_BYTES);
        updateEdenTime();

        UNSAFE.putDouble(eden.address + eden.position, v);
        long sequence = eden.position + eden.startSequence;
        eden.position += DOUBLE_SIZE_IN_BYTES;
        eden.count++;
        return sequence;
    }

    public double getDouble(long sequence) {
        return UNSAFE.getDouble(toAddress(sequence));
    }

    @Override
    public long putObject(Double o) {
        return putDouble(o);
    }

    @Override
    public Double getObject(long sequence) {
        return getDouble(sequence);
    }

    @Override
    public Double reduce(BinaryOperator<Double> accumulator) {
        throw new UnsupportedOperationException();
    }
}
