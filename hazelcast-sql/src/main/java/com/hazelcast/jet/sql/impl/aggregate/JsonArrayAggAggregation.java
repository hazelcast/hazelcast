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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class JsonArrayAggAggregation implements SqlAggregation {
    private List<Object> objects = new ArrayList<>();
    private boolean isAbsentOnNull;

    private JsonArrayAggAggregation() {
    }

    private JsonArrayAggAggregation(boolean isAbsentOnNull) {
        this.isAbsentOnNull = isAbsentOnNull;
    }

    public static JsonArrayAggAggregation create(boolean isAbsentOnNull) {
        return new JsonArrayAggAggregation(isAbsentOnNull);
    }

    @Override
    public void accumulate(Object value) {
        objects.add(value);
    }

    @Override
    public void combine(SqlAggregation other) {
        JsonArrayAggAggregation otherC = (JsonArrayAggAggregation) other;
        objects.addAll(otherC.objects);
    }

    @Override
    public Object collect() {
        StringBuilder sb = new StringBuilder();
        boolean firstValue = true;
        sb.append("[");
        for (Object value : objects) {
            if (value == null) {
                if (!isAbsentOnNull) {
                    if (!firstValue) {
                        sb.append(", ");
                    }
                    sb.append("null");
                    firstValue = false;
                }
            } else {
                if (!firstValue) {
                    sb.append(", ");
                }
                sb.append(value);
                firstValue = false;
            }
        }
        sb.append("]");

        return new HazelcastJsonValue(sb.toString());
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeBoolean(isAbsentOnNull);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        isAbsentOnNull = in.readBoolean();
    }
}
