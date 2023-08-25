/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl.type;

import com.hazelcast.jet.sql.impl.JetSqlSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.io.Serializable;

/**
 * Day-second interval.
 */
public class SqlDaySecondInterval implements IdentifiedDataSerializable, Comparable<SqlDaySecondInterval>, Serializable {

    private long millis;

    public SqlDaySecondInterval() {
        // No-op.
    }

    public SqlDaySecondInterval(long millis) {
        this.millis = millis;
    }

    public long getMillis() {
        return millis;
    }

    @Override
    public int getFactoryId() {
        return JetSqlSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return JetSqlSerializerHook.INTERVAL_DAY_SECOND;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(millis);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        millis = in.readLong();
    }

    @Override
    public int hashCode() {
        return Long.hashCode(millis);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SqlDaySecondInterval) {
            SqlDaySecondInterval other = ((SqlDaySecondInterval) obj);

            return millis == other.millis;
        }

        return false;
    }

    @Override
    public int compareTo(SqlDaySecondInterval other) {
        return Long.compare(millis, other.millis);
    }

    @Override
    public String toString() {
        return "SqlDaySecondInterval{millis=" + millis + "}";
    }
}
