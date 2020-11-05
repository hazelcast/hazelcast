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

package com.hazelcast.jet.sql.impl.aggregate;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.Objects;

@NotThreadSafe
public class CountSqlAggregation extends SqlAggregation {

    private long value;

    public CountSqlAggregation() {
        super(-1, false, false);
    }

    public CountSqlAggregation(int index) {
        super(index, true, false);
    }

    public CountSqlAggregation(int index, boolean distinct) {
        super(index, true, distinct);
    }

    @Override
    public QueryDataType resultType() {
        return QueryDataType.BIGINT;
    }

    @Override
    protected void accumulate(Object value) {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CountSqlAggregation that = (CountSqlAggregation) o;
        return value == that.value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
}
