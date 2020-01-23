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

package com.hazelcast.log.encoders;

import com.hazelcast.internal.memory.impl.UnsafeUtil;
import com.hazelcast.log.TimeSeriesEntry;
import sun.misc.Unsafe;

import static com.hazelcast.internal.nio.Bits.DOUBLE_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.LONG_SIZE_IN_BYTES;

public class TimeEntryEncoder implements Encoder<TimeSeriesEntry> {
    private final static Unsafe UNSAFE = UnsafeUtil.UNSAFE;

    @Override
    public int encode(long address, int available, TimeSeriesEntry entry) {
        int required = LONG_SIZE_IN_BYTES + DOUBLE_SIZE_IN_BYTES;

        if (available < required) {
            return -required;
        }

        UNSAFE.putLong(address, entry.getTimestamp());
        UNSAFE.putDouble(address + LONG_SIZE_IN_BYTES, entry.getValue());

        return required;
    }

    @Override
    public TimeSeriesEntry decode(long address, Consumed consumed) {
        long timestamp = UNSAFE.getLong(address);
        double value = UNSAFE.getDouble(address + LONG_SIZE_IN_BYTES);
        consumed.value = LONG_SIZE_IN_BYTES + DOUBLE_SIZE_IN_BYTES;
        return new TimeSeriesEntry(timestamp, value);
    }
}
