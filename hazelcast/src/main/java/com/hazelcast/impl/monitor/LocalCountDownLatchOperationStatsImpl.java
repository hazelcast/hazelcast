/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl.monitor;

import com.hazelcast.monitor.LocalCountDownLatchOperationStats;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LocalCountDownLatchOperationStatsImpl extends LocalOperationStatsSupport
        implements LocalCountDownLatchOperationStats {

    long numberOfAwaitsReleased;
    long numberOfGatesOpened;
    OperationStat await = new OperationStat(0, 0);
    OperationStat countdown = new OperationStat(0, 0);
    OperationStat other = new OperationStat(0, 0);

    void writeDataInternal(DataOutput out) throws IOException {
        await.writeData(out);
        countdown.writeData(out);
        other.writeData(out);
    }

    void readDataInternal(DataInput in) throws IOException {
        (await = new OperationStat()).readData(in);
        (countdown = new OperationStat()).readData(in);
        (other = new OperationStat()).readData(in);
    }

    public long total() {
        return await.count + countdown.count + other.count;
    }

    public long getNumberOfAwaits() {
        return await.count;
    }

    public long getNumberOfCountDowns() {
        return countdown.count;
    }

    public long getNumberOfOthers() {
        return other.count;
    }

    public long getTotalAwaitLatency() {
        return await.totalLatency;
    }

    public long getTotalCountDownLatency() {
        return countdown.totalLatency;
    }

    public long getTotalOtherLatency() {
        return other.totalLatency;
    }

    public long getNumberOfAwaitsReleased() {
        return numberOfAwaitsReleased;
    }

    public long getNumberOfGatesOpened() {
        return numberOfGatesOpened;
    }

    public String toString() {
        return "LocalCountDownLatchOperationStats{" +
                "total= " + total() +
                ", await:" + await +
                ", countdown:" + countdown +
                ", other:" + other + "}";
    }
}
