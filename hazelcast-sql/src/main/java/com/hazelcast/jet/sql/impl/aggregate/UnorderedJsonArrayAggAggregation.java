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

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.ArrayList;

@NotThreadSafe
public final class UnorderedJsonArrayAggAggregation implements SqlAggregation {
    private final ArrayList<Object> values = new ArrayList<>();
    private boolean isAbsentOnNull;

    @SuppressWarnings("unused") // used for deserialization
    private UnorderedJsonArrayAggAggregation() {
    }

    private UnorderedJsonArrayAggAggregation(boolean isAbsentOnNull) {
        this.isAbsentOnNull = isAbsentOnNull;
    }

    public static UnorderedJsonArrayAggAggregation create(boolean isAbsentOnNull) {
        return new UnorderedJsonArrayAggAggregation(isAbsentOnNull);
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
        out.writeInt(values.size());
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
