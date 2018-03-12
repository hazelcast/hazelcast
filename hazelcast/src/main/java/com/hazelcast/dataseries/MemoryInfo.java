/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.dataseries;

import java.io.Serializable;

/**
 *
 */
public class MemoryInfo implements Serializable {
    private final long consumedBytes;
    private final long allocatedBytes;
    private final int segmentsInUse;
    private final long count;

    public MemoryInfo(long consumedBytes, long allocatedBytes, int segmentsInUse, long count) {
        this.consumedBytes = consumedBytes;
        this.allocatedBytes = allocatedBytes;
        this.segmentsInUse = segmentsInUse;
        this.count = count;
    }

    public int segmentsInUse() {
        return segmentsInUse;
    }

    public long consumedBytes() {
        return consumedBytes;
    }

    public long allocatedBytes() {
        return allocatedBytes;
    }

    public long count() {
        return count;
    }

    @Override
    public String toString() {
        return "MemoryInfo{"
                + "count=" + count
                + ", consumedBytes=" + consumedBytes
                + ", allocatedBytes=" + allocatedBytes
                + ", storageEfficiency=" + ((100d * consumedBytes) / allocatedBytes) + "%"
                + ", segmentsInUse=" + segmentsInUse
                + '}';
    }
}
