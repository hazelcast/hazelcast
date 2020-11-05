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
import com.hazelcast.nio.serialization.DataSerializable;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.Arrays;

/**
 * A holder of {@link SqlAggregation} instances for the entire row.
 */
@NotThreadSafe
public class SqlAggregations implements DataSerializable {

    private SqlAggregation[] aggregations;

    @SuppressWarnings("unused")
    private SqlAggregations() {
    }

    public SqlAggregations(SqlAggregation[] aggregations) {
        this.aggregations = aggregations;
    }

    public void accumulate(Object[] row) {
        for (SqlAggregation aggregation : aggregations) {
            aggregation.accumulate(row);
        }
    }

    public void combine(SqlAggregations other) {
        assert aggregations.length == other.aggregations.length;

        for (int i = 0; i < aggregations.length; i++) {
            aggregations[i].combine(other.aggregations[i]);
        }
    }

    public Object[] collect() {
        Object[] values = new Object[aggregations.length];
        for (int i = 0; i < aggregations.length; i++) {
            values[i] = aggregations[i].collect();
        }
        return values;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(aggregations.length);
        for (SqlAggregation aggregation : aggregations) {
            out.writeObject(aggregation);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        aggregations = new SqlAggregation[in.readInt()];
        for (int i = 0; i < aggregations.length; i++) {
            aggregations[i] = in.readObject();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SqlAggregations that = (SqlAggregations) o;
        return Arrays.equals(aggregations, that.aggregations);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(aggregations);
    }
}
