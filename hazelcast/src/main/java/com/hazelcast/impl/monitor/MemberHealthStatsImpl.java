/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

import com.hazelcast.monitor.MemberHealthStats;
import com.hazelcast.nio.DataSerializable;
import com.hazelcast.util.ThreadStats;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MemberHealthStatsImpl implements MemberHealthStats, DataSerializable {

    boolean outOfMemory;
    boolean active;
    ThreadStats serviceThreadStats = new ThreadStats(0, 0, 0, true);
    ThreadStats inThreadStats = new ThreadStats(0, 0, 0, true);
    ThreadStats outThreadStats = new ThreadStats(0, 0, 0, true);

    public void readData(DataInput in) throws IOException {
        outOfMemory = in.readBoolean();
        active = in.readBoolean();
        serviceThreadStats = readThreadStats(in);
        inThreadStats = readThreadStats(in);
        outThreadStats = readThreadStats(in);
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeBoolean(outOfMemory);
        out.writeBoolean(active);
        writeThreadStats(out, serviceThreadStats);
        writeThreadStats(out, inThreadStats);
        writeThreadStats(out, outThreadStats);
    }

    void writeThreadStats(DataOutput out, ThreadStats t) throws IOException {
        out.writeInt(t.getUtilizationPercentage());
        out.writeLong(t.getRunCount());
        out.writeLong(t.getWaitCount());
        out.writeBoolean(t.isRunning());
    }

    ThreadStats readThreadStats(DataInput in) throws IOException {
        return new ThreadStats(in.readInt(), in.readLong(), in.readLong(), in.readBoolean());
    }

    public ThreadStats getInThreadStats() {
        return inThreadStats;
    }

    public void setInThreadStats(ThreadStats inThreadStats) {
        this.inThreadStats = inThreadStats;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public boolean isOutOfMemory() {
        return outOfMemory;
    }

    public void setOutOfMemory(boolean outOfMemory) {
        this.outOfMemory = outOfMemory;
    }

    public ThreadStats getOutThreadStats() {
        return outThreadStats;
    }

    public void setOutThreadStats(ThreadStats outThreadStats) {
        this.outThreadStats = outThreadStats;
    }

    public ThreadStats getServiceThreadStats() {
        return serviceThreadStats;
    }

    public void setServiceThreadStats(ThreadStats serviceThreadStats) {
        this.serviceThreadStats = serviceThreadStats;
    }

    @Override
    public String toString() {
        return "MemberHealthStatsImpl{" +
                "outOfMemory=" + outOfMemory +
                ", active=" + active +
                ", serviceThreadStats=" + serviceThreadStats +
                ", inThreadStats=" + inThreadStats +
                ", outThreadStats=" + outThreadStats +
                '}';
    }
}
