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

package com.hazelcast.sql.impl.expression;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;
import java.util.Objects;

public class ConstantPredicateExpression implements Expression<Boolean> {

    private boolean value;

    public ConstantPredicateExpression() {
        // No-op.
    }

    public ConstantPredicateExpression(boolean value) {
        this.value = value;
    }

    @Override
    public Boolean eval(Row row, ExpressionEvalContext context) {
        return value;
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.BOOLEAN;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeBoolean(value);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        value = in.readBoolean();
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ConstantPredicateExpression that = (ConstantPredicateExpression) o;

        return value == that.value;
    }
}
