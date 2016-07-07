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

import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.internal.memory.impl.EndiannessUtil;

import static com.hazelcast.internal.memory.impl.EndiannessUtil.CUSTOM_ACCESS;

/**
 * Accumulates float values into a float value.
 */
public abstract class FloatAccumulator implements Accumulator {
    @Override
    public void accept(
            MemoryAccessor memoryAccessor, long oldAddress, long oldSize,
            long newAddress, long newSize, boolean useBigEndian
    ) {
        long oldDataAddress = toDataAddress(oldAddress);
        long newDataAddress = toDataAddress(newAddress);

        float oldValue = EndiannessUtil.readFloat(CUSTOM_ACCESS,
                memoryAccessor, oldDataAddress, useBigEndian);
        float newValue = EndiannessUtil.readFloat(CUSTOM_ACCESS,
                memoryAccessor, newDataAddress, useBigEndian);
        EndiannessUtil.writeFloat(CUSTOM_ACCESS,
                memoryAccessor, oldDataAddress, apply(oldValue, newValue), useBigEndian);
    }

    protected abstract float apply(float oldValue, float newValue);
}
