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

package com.hazelcast.sql.impl.memory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Global reservation manager which controls total memory consumption.
 */
public class GlobalMemoryReservationManager {
    private static final double HIGH_PRESSURE_THRESHOLD = 0.25d;
    private static final double MEDIUM_PRESSURE_THRESHOLD = 0.5d;

    // TODO: That will be a source of contention. Use TLB?
    private final AtomicLong allocated = new AtomicLong();
    private final long limit;

    public GlobalMemoryReservationManager(long limit) {
        this.limit = limit > 0 ? limit : 0;
    }

    public long reserve(long size) {
        if (limit > 0) {
            long allocatedBefore = allocated.get();

            if (allocatedBefore + size <= limit) {
                long allocatedAfter = allocated.addAndGet(size);

                if (allocatedAfter <= limit) {
                    return size;
                }
            }

            return -1;
        } else {
            allocated.addAndGet(size);

            return size;
        }
    }

    public void release(long size) {
        allocated.addAndGet(-size);
    }

    public double getFreeMemoryFraction() {
        return limit > 0 ? (double) allocated.get() / limit : 1.0d;
    }

    public MemoryPressure getMemoryPressure() {
        double freeMemory = getFreeMemoryFraction();

        return freeMemory < HIGH_PRESSURE_THRESHOLD
            ? MemoryPressure.HIGH : freeMemory < MEDIUM_PRESSURE_THRESHOLD ? MemoryPressure.MEDIUM : MemoryPressure.LOW;
    }
}
