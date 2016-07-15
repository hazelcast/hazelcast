/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.memory.binarystorage.accumulator;

import com.hazelcast.nio.Bits;

/**
 * Sums int values into an int value
 */
public class IntSumAccumulator extends IntAccumulator {

    @Override
    public boolean isAssociative() {
        return true;
    }

    @Override
    public long toDataAddress(long address) {
        //                    Value type
        return address + Bits.BYTE_SIZE_IN_BYTES;
    }

    @Override
    protected int apply(int oldValue, int newValue) {
        return oldValue + newValue;
    }
}
