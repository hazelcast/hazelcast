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

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.expression.json.JsonCreationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.row.JetSqlRow;

import java.io.IOException;
import java.util.PriorityQueue;

public final class JsonArrayAggAggregation implements SqlAggregation {
    private PriorityQueue<JetSqlRow> objects;
    private ExpressionUtil.SqlRowComparator comparator;
    private boolean isAbsentOnNull;
    private int aggIndex;

    private JsonArrayAggAggregation() {
    }

    private JsonArrayAggAggregation(ExpressionUtil.SqlRowComparator comparator, boolean isAbsentOnNull, int aggIndex) {
        this.comparator = comparator;
        this.isAbsentOnNull = isAbsentOnNull;
        this.aggIndex = aggIndex;
        this.objects = new PriorityQueue<>(this.comparator);
    }

    public static JsonArrayAggAggregation create(
            ExpressionUtil.SqlRowComparator comparator,
            boolean isAbsentOnNull,
            int aggIndex
    ) {
        return new JsonArrayAggAggregation(comparator, isAbsentOnNull, aggIndex);
    }

    @Override
    public void accumulate(Object value) {
        JetSqlRow row = (JetSqlRow) value;
        objects.add(row);
    }

    @Override
    public void combine(SqlAggregation other) {
        JsonArrayAggAggregation otherC = (JsonArrayAggAggregation) other;
        while (!otherC.objects.isEmpty()) {
            objects.add(otherC.objects.poll());
        }
    }

    @Override
    public Object collect() {
        StringBuilder sb = new StringBuilder();
        boolean firstValue = true;
        sb.append("[");
        while (!objects.isEmpty()) {
            JetSqlRow row = objects.poll();
            Object value = row.get(aggIndex);
            if (value == null) {
                if (!isAbsentOnNull) {
                    if (!firstValue) {
                        sb.append(",");
                    }
                    sb.append("null");
                    firstValue = false;
                }
            } else {
                if (!firstValue) {
                    sb.append(",");
                }
                sb.append(JsonCreationUtil.serializeValue(value));
                firstValue = false;
            }
        }
        sb.append("]");

        return new HazelcastJsonValue(sb.toString());
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        throw new UnsupportedOperationException("Should not be called");
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        throw new UnsupportedOperationException("Should not be called");
    }
}
