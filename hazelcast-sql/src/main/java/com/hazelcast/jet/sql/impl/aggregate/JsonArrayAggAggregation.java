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

import com.google.common.collect.TreeMultiset;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.expression.json.JsonCreationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.row.JetSqlRow;

import java.io.IOException;
import java.util.ArrayList;

public final class JsonArrayAggAggregation {

    private JsonArrayAggAggregation() {
    }

    public static OrderedJsonArrayAggAggregation createOrdered(
            ExpressionUtil.SqlRowComparator comparator,
            boolean isAbsentOnNull,
            int aggIndex
    ) {
        return new OrderedJsonArrayAggAggregation(comparator, isAbsentOnNull, aggIndex);
    }

    public static UnorderedJsonArrayAggAggregation createUnordered(boolean isAbsentOnNull) {
        return new UnorderedJsonArrayAggAggregation(isAbsentOnNull);
    }

    private static final class OrderedJsonArrayAggAggregation implements SqlAggregation {
        private final TreeMultiset<JetSqlRow> objects;
        private final boolean isAbsentOnNull;
        private final int aggIndex;

        private OrderedJsonArrayAggAggregation(ExpressionUtil.SqlRowComparator comparator, boolean isAbsentOnNull, int aggIndex) {
            this.isAbsentOnNull = isAbsentOnNull;
            this.aggIndex = aggIndex;
            this.objects = TreeMultiset.create(comparator);
        }

        @Override
        public void accumulate(Object value) {
            JetSqlRow row = (JetSqlRow) value;
            objects.add(row);
        }

        @Override
        public void combine(SqlAggregation other) {
            throw new UnsupportedOperationException("OrderedJsonArrayAgg combine() method should not be called");
        }

        @Override
        public Object collect() {
            StringBuilder sb = new StringBuilder();
            boolean firstValue = true;
            sb.append("[");
            for (JetSqlRow row : objects) {
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
            if (firstValue) {
                return null;
            } else {
                return new HazelcastJsonValue(sb.toString());
            }
        }

        @Override
        public void writeData(ObjectDataOutput out) {
            throw new UnsupportedOperationException("OrderedJsonArrayAgg writeData() method should not be called");
        }

        @Override
        public void readData(ObjectDataInput in) {
            throw new UnsupportedOperationException("OrderedJsonArrayAgg readData() should not be called");
        }
    }

    private static final class UnorderedJsonArrayAggAggregation implements SqlAggregation {
        private final ArrayList<Object> values = new ArrayList<>();
        private boolean isAbsentOnNull;

        @SuppressWarnings("unused") // used for deserialization
        private UnorderedJsonArrayAggAggregation() {
        }

        private UnorderedJsonArrayAggAggregation(boolean isAbsentOnNull) {
            this.isAbsentOnNull = isAbsentOnNull;
        }

        @Override
        public void accumulate(Object value) {
            values.add(value);
        }

        @Override
        public void combine(SqlAggregation other) {
            UnorderedJsonArrayAggAggregation other0 = (UnorderedJsonArrayAggAggregation) other;
            values.addAll(other0.values);
        }

        @Override
        public Object collect() {
            StringBuilder sb = new StringBuilder();
            boolean firstValue = true;
            sb.append("[");
            for (Object value : values) {
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
            if (firstValue) {
                return null;
            } else {
                return new HazelcastJsonValue(sb.toString());
            }
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeBoolean(isAbsentOnNull);
            out.write(values.size());
            for (Object o : values) {
                out.writeObject(o);
            }
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            isAbsentOnNull = in.readBoolean();
            int size = in.readInt();
            values.ensureCapacity(size);
            for (int i = 0; i < size; i++) {
                values.add(in.readObject());
            }
        }
    }
}
