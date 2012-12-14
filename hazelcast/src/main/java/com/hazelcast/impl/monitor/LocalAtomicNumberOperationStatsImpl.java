/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.impl.monitor;

import com.hazelcast.monitor.LocalAtomicNumberOperationStats;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LocalAtomicNumberOperationStatsImpl extends LocalOperationStatsSupport
        implements LocalAtomicNumberOperationStats {

    OperationStat modified = new OperationStat(0, 0);
    OperationStat nonModified = new OperationStat(0, 0);

    void writeDataInternal(DataOutput out) throws IOException {
        modified.writeData(out);
        nonModified.writeData(out);
    }

    void readDataInternal(DataInput in) throws IOException {
        (modified = new OperationStat()).readData(in);
        (nonModified = new OperationStat()).readData(in);
    }

    public long total() {
        return modified.count + nonModified.count;
    }

    public long getNumberOfModifyOps() {
        return modified.count;
    }

    public long getNumberOfNonModifyOps() {
        return nonModified.count;
    }

    public long getTotalAcquireLatency() {
        return modified.totalLatency;
    }

    public long getTotalNonAcquireLatency() {
        return nonModified.totalLatency;
    }

    public String toString() {
        return "LocalSemaphoreOperationStats{" +
                "total= " + total() +
                ", modified:" + modified +
                ", nonModified:" + nonModified + "}";
    }
}
