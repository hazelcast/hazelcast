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

package com.hazelcast.sql.impl.type;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

/**
 * Interval data type.
 */
public class SqlDaySecondInterval implements DataSerializable, Comparable<SqlDaySecondInterval> {
    /** Seconds. */
    private long seconds;

    /** Nanos. */
    private int nanos;

    public SqlDaySecondInterval() {
        // No-op.
    }

    public SqlDaySecondInterval(long seconds, int nanos) {
        this.seconds = seconds;
        this.nanos = nanos;
    }

    public long getSeconds() {
        return seconds;
    }

    public int getNanos() {
        return nanos;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(seconds);
        out.writeInt(nanos);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        seconds = in.readLong();
        nanos = in.readInt();
    }

    @Override
    public int hashCode() {
        return 31 * Long.hashCode(seconds) + nanos;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SqlDaySecondInterval) {
            SqlDaySecondInterval other = ((SqlDaySecondInterval) obj);

            return seconds == other.seconds && nanos == other.nanos;
        }

        return false;
    }

    @Override
    public int compareTo(SqlDaySecondInterval other) {
        int res = Long.compare(seconds, other.seconds);

        if (res == 0) {
            res = Integer.compare(nanos, other.nanos);
        }

        return res;
    }

    @Override
    public String toString() {
        return "SqlDaySecondInterval{seconds=" + seconds + ", nanos=" + nanos + "}";
    }
}
