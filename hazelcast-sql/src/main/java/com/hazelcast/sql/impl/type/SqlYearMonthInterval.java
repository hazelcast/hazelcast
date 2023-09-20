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
 * Year-month interval.
 */
public class SqlYearMonthInterval implements IdentifiedDataSerializable, Comparable<SqlYearMonthInterval>, Serializable {

    private int months;

    public SqlYearMonthInterval() {
        // No-op.
    }

    public SqlYearMonthInterval(int months) {
        this.months = months;
    }

    public int getMonths() {
        return months;
    }

    @Override
    public int getFactoryId() {
        return JetSqlSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return JetSqlSerializerHook.INTERVAL_YEAR_MONTH;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(months);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        months = in.readInt();
    }

    @Override
    public int hashCode() {
        return months;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SqlYearMonthInterval) {
            SqlYearMonthInterval other = ((SqlYearMonthInterval) obj);

            return months == other.months;
        }

        return false;
    }

    @Override
    public int compareTo(SqlYearMonthInterval other) {
        return Integer.compare(months, other.months);
    }

    @Override
    public String toString() {
        return "SqlYearMonthInterval{months=" + months + "}";
    }
}
