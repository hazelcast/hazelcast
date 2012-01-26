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

import com.hazelcast.monitor.LocalInstanceOperationStats;
import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

abstract class LocalOperationStatsSupport implements LocalInstanceOperationStats {

    long periodStart;
    long periodEnd;

    public final long getPeriodStart() {
        return periodStart;
    }

    public final long getPeriodEnd() {
        return periodEnd;
    }

    public final void writeData(DataOutput out) throws IOException {
        out.writeLong(periodStart);
        out.writeLong(periodEnd);
        writeDataInternal(out);
    }

    abstract void writeDataInternal(DataOutput out) throws IOException;

    public final void readData(DataInput in) throws IOException {
        periodStart = in.readLong();
        periodEnd = in.readLong();
        readDataInternal(in);
    }

    abstract void readDataInternal(DataInput in) throws IOException;

    class OperationStat implements DataSerializable {
        long count;
        long totalLatency;

        public OperationStat() {
            this(0, 0);
        }

        public OperationStat(long c, long l) {
            this.count = c;
            this.totalLatency = l;
        }

        @Override
        public String toString() {
            return "OperationStat{" + "count=" + count + ", averageLatency="
                    + ((count == 0) ? 0 : totalLatency / count) + '}';
        }

        public void writeData(DataOutput out) throws IOException {
            out.writeLong(count);
            out.writeLong(totalLatency);
        }

        public void readData(DataInput in) throws IOException {
            count = in.readLong();
            totalLatency = in.readLong();
        }

        public void add(long c, long l) {
            count += c;
            totalLatency += l;
        }
    }
}
