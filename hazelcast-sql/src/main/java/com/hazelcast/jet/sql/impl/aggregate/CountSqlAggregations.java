/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.aggregate;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;

public final class CountSqlAggregations {

    private CountSqlAggregations() {
    }

    public static SqlAggregation from(boolean ignoreNulls, boolean distinct) {
        SqlAggregation aggregation = ignoreNulls ? new CountIgnoreNullsSqlAggregation() : new CountSqlAggregation();
        return distinct ? new DistinctSqlAggregation(aggregation) : aggregation;
    }

    @NotThreadSafe
    private static class CountSqlAggregation implements SqlAggregation {

        private long value;

        @Override
        public void accumulate(Object value) {
            this.value++;
        }

        @Override
        public void combine(SqlAggregation other0) {
            CountSqlAggregation other = (CountSqlAggregation) other0;

            value += other.value;
        }

        @Override
        public Object collect() {
            return value;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeLong(value);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            value = in.readLong();
        }
    }

    @NotThreadSafe
    private static final class CountIgnoreNullsSqlAggregation extends CountSqlAggregation {

        @Override
        public void accumulate(Object value) {
            if (value == null) {
                return;
            }

            super.accumulate(value);
        }
    }
}
