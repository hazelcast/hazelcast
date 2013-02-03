/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.monitor.LocalLockOperationStats;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LocalLockOperationStatsImpl extends LocalOperationStatsSupport
        implements LocalLockOperationStats {

    long numberOfLocks;
    long numberOfUnlocks;
    long numberOfFailedLocks;

    public long getNumberOfLocks() {
        return numberOfLocks;
    }

    public long getNumberOfUnlocks() {
        return numberOfUnlocks;
    }

    public long getNumberOfFailedLocks() {
        return numberOfFailedLocks;
    }

    public long total() {
        return numberOfLocks + numberOfUnlocks + numberOfFailedLocks;
    }

    void writeDataInternal(DataOutput out) throws IOException {
        out.writeLong(numberOfLocks);
        out.writeLong(numberOfUnlocks);
        out.writeLong(numberOfFailedLocks);
    }

    void readDataInternal(DataInput in) throws IOException {
        numberOfLocks = in.readLong();
        numberOfUnlocks = in.readLong();
        numberOfFailedLocks = in.readLong();
    }
}
