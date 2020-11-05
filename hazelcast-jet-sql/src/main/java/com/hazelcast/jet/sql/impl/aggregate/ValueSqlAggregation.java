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

/**
 * Special-case aggregation that aggregates multiple instances of the same
 * value. The result is the value. Asserts that no non-equal value is
 * accumulated.
 */
@NotThreadSafe
public class ValueSqlAggregation extends SqlAggregation {

    private QueryDataType operandType;

    private Object value;

    @SuppressWarnings("unused")
    private ValueSqlAggregation() {
    }

    public ValueSqlAggregation(int index, QueryDataType operandType) {
        super(index, false, false);
        this.operandType = operandType;
    }

    @Override
    public QueryDataType resultType() {
        return operandType;
    }

    @Override
    protected void accumulate(Object value) {
        assert this.value == null || this.value.equals(value);

        this.value = value;
    }

    @Override
    public void combine(SqlAggregation other0) {
        ValueSqlAggregation other = (ValueSqlAggregation) other0;

        Object value = other.value;

        assert this.value == null || this.value.equals(value);

        this.value = value;
    }

    @Override
    public Object collect() {
        return value;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(operandType);
        out.writeObject(value);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        operandType = in.readObject();
        value = in.readObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ValueSqlAggregation that = (ValueSqlAggregation) o;
        return Objects.equals(operandType, that.operandType) &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operandType, value);
    }
}
