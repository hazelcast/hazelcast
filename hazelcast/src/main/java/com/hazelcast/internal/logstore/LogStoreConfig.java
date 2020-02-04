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

import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.UnsafeAllocator;
import com.hazelcast.log.encoders.Encoder;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkPositive;

public class LogStoreConfig {
    private int segmentSize = 4 * 1024 * 1024;
    private Encoder encoder;
    private int maxSegmentCount = Integer.MAX_VALUE;
    private MemoryAllocator allocator = UnsafeAllocator.INSTANCE;
    private Class type = Object.class;
    private long tenuringAgeMillis = Long.MAX_VALUE;
    private long retentionMillis = Long.MAX_VALUE;

    public LogStoreConfig setEncoder(Encoder encoder) {
        this.encoder = checkNotNull(encoder, "encoder can't be null");
        return this;
    }

    public Class getType() {
        return type;
    }

    public LogStoreConfig setType(Class type) {
        this.type = checkNotNull(type);
        return this;
    }

    public int getSegmentSize() {
        return segmentSize;
    }

    public Encoder getEncoder() {
        return encoder;
    }

    public int getMaxSegmentCount() {
        return maxSegmentCount;
    }

    public LogStoreConfig setSegmentSize(int segmentSize) {
        if (segmentSize < 1) {
            throw new IllegalStateException();
        }
        this.segmentSize = segmentSize;
        return this;
    }

    public LogStoreConfig setMaxSegmentCount(int maxSegmentCount) {
        if (segmentSize < 0) {
            throw new IllegalStateException();
        }
        this.maxSegmentCount = maxSegmentCount;
        return this;
    }

    public MemoryAllocator getAllocator() {
        return allocator;
    }

    public LogStoreConfig setAllocator(MemoryAllocator allocator) {
        this.allocator = checkNotNull(allocator, "allocator can't be null");
        return this;
    }

    public LogStoreConfig setTenuringAgeMillis(long tenuringAgeMillis) {
        checkPositive(tenuringAgeMillis, "tenuringAgeMillis can't be smaller than 0");
        this.tenuringAgeMillis = tenuringAgeMillis;
        return this;
    }

    public long getTenuringAgeMillis() {
        return tenuringAgeMillis;
    }

    public long getRetentionMillis() {
        return retentionMillis;
    }

    public LogStoreConfig setRetentionMillis(long retentionMillis) {
        checkPositive(retentionMillis, "retentionMillis can't be smaller than 0");
        this.retentionMillis = retentionMillis;
        return this;
    }
}
