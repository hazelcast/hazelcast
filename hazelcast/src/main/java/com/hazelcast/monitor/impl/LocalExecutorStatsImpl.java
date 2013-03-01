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

package com.hazelcast.monitor.impl;

import com.hazelcast.monitor.LocalExecutorOperationStats;
import com.hazelcast.monitor.LocalExecutorStats;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class LocalExecutorStatsImpl extends LocalInstanceStatsSupport<LocalExecutorOperationStats> implements LocalExecutorStats {

    private long creationTime;
    private long totalStarted;
    private long totalFinished;

    @Override
    LocalExecutorOperationStats newOperationStatsInstance() {
        return new LocalExecutorOperationStatsImpl();
    }

    @Override
    void writeDataInternal(ObjectDataOutput out) throws IOException {
        out.writeLong(creationTime);
        out.writeLong(totalStarted);
        out.writeLong(totalFinished);
    }

    @Override
    void readDataInternal(ObjectDataInput in) throws IOException {
        creationTime = in.readLong();
        totalStarted = in.readLong();
        totalFinished = in.readLong();
    }


    public long getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(long creationTime) {
        this.creationTime = creationTime;
    }

    public long getTotalStarted() {
        return totalStarted;
    }

    public void setTotalStarted(long totalStarted) {
        this.totalStarted = totalStarted;
    }

    public long getTotalFinished() {
        return totalFinished;
    }

    public void setTotalFinished(long totalFinished) {
        this.totalFinished = totalFinished;
    }

    @Override
    public String toString() {
        return "LocalExecutorStatsImpl{" +
                "creationTime=" + creationTime +
                ", totalStarted=" + totalStarted +
                ", totalFinished=" + totalFinished +
                ", operationStats=" + getOperationStats() +
                '}';
    }
}
