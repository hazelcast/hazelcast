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

package com.hazelcast.log.impl;

import com.hazelcast.internal.logstore.LogStore;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.log.CloseableIterator;
import com.hazelcast.log.encoders.HeapDataEncoder;

import java.io.IOException;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;

public class LogContainer {
    private final SerializationService serializationService;
    private String name;
    private int partition;
    private LogStore logStorage;

    public LogContainer(String name, int partition, LogStore logStorage, SerializationService ss) {
        this.name = name;
        this.partition = partition;
        this.logStorage = logStorage;
        this.serializationService = ss;
    }

    public Object get(long sequence) {
        return logStorage.getObject(sequence);
    }

    public long put(Data data) {
        if (logStorage.config().getEncoder() instanceof HeapDataEncoder) {
            return logStorage.putObject(data);
        } else {
            return logStorage.putObject(serializationService.toObject(data));
        }
    }

    public Object reduce(BinaryOperator accumulator) {
        return logStorage.reduce(accumulator);
    }

    public void putMany(Supplier supplier) {
        if (logStorage.config().getEncoder() instanceof HeapDataEncoder) {
            Object item = supplier.get();
            while (item != null) {
                logStorage.putObject(serializationService.toData(item));
                item = supplier.get();
            }
        } else {
            Object item = supplier.get();
            while (item != null) {
                logStorage.putObject(item);
                item = supplier.get();
            }
        }
    }

    public UsageInfo usage() {
        UsageInfo usageInfo = new UsageInfo();
        usageInfo.segments = logStorage.segmentCount();
        usageInfo.bytesInUse = logStorage.bytesInUse();
        usageInfo.bytesAllocated = logStorage.bytesAllocated();
        usageInfo.count = logStorage.count();
        return usageInfo;
    }

    public int segmentCount() {
        return logStorage.segmentCount();
    }

    public long count() {
        return logStorage.count();
    }

    public void clear() {
        logStorage.clear();
    }

    public void touch() {
        logStorage.touch();
    }

    public CloseableIterator iterator() {
       return logStorage.iterator();
    }


}
