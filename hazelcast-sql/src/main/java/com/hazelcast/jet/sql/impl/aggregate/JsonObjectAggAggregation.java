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

package com.hazelcast.jet.sql.impl.aggregate;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.jet.sql.impl.expression.json.JsonCreationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.row.JetSqlRow;
import org.apache.calcite.util.Pair;

import java.io.IOException;
import java.util.ArrayList;

public class JsonObjectAggAggregation implements SqlAggregation {
    private final ArrayList<Pair<Object, Object>> keyValues = new ArrayList<>();
    private int keyIndex;
    private int valueIndex;
    private boolean isAbsentOnNull;

    @SuppressWarnings("unused") // for deserialization
    public JsonObjectAggAggregation() {
    }

    public JsonObjectAggAggregation(int keyIndex, int valueIndex, boolean isAbsentOnNull) {
        this.keyIndex = keyIndex;
        this.valueIndex = valueIndex;
        this.isAbsentOnNull = isAbsentOnNull;
    }

    @Override
    public void accumulate(Object value) {
        JetSqlRow row = (JetSqlRow) value;
        Pair<Object, Object> pair = new Pair<>(row.get(keyIndex), row.get(valueIndex));
        if (pair.getKey() == null) {
            throw QueryException.error("NULL key is not supported for JSON_OBJECTAGG");
        }
        keyValues.add(pair);
    }

    @Override
    public void combine(SqlAggregation other) {
        JsonObjectAggAggregation other0 = (JsonObjectAggAggregation) other;
        keyValues.addAll(other0.keyValues);
    }

    @Override
    public Object collect() {
        StringBuilder sb = new StringBuilder();
        boolean firstValue = true;

        sb.append("{");
        for (Pair<Object, Object> pair : keyValues) {
            Object value = pair.getValue();
            if (value == null && isAbsentOnNull) {
                continue;
            }
            if (firstValue) {
                firstValue = false;
            } else {
                sb.append(",");
            }
            sb.append(JsonCreationUtil.serializeValue(pair.getKey()));
            sb.append(":");
            sb.append(JsonCreationUtil.serializeValue(value));
        }

        sb.append("}");
        if (firstValue) {
            return null;
        } else {
            return new HazelcastJsonValue(sb.toString());
        }
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(keyIndex);
        out.writeInt(valueIndex);
        out.writeBoolean(isAbsentOnNull);
        out.writeInt(keyValues.size());
        for (Pair<Object, Object> entry : keyValues) {
            out.writeObject(entry.getKey());
            out.writeObject(entry.getValue());
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        keyIndex = in.readInt();
        valueIndex = in.readInt();
        isAbsentOnNull = in.readBoolean();
        int size = in.readInt();
        keyValues.ensureCapacity(size);
        for (int i = 0; i < size; i++) {
            Object key = in.readObject();
            Object value = in.readObject();
            keyValues.add(new Pair<>(key, value));
        }
    }
}
