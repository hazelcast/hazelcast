/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.expression.datetime;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.UniExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;
import java.util.Objects;

public class ExtractFunction extends UniExpression<Double> implements IdentifiedDataSerializable {

    private ExtractField extractField;

    public ExtractFunction() { }

    private ExtractFunction(Expression<?> time, ExtractField extractField) {
        super(time);
        this.extractField = extractField;
    }

    public static ExtractFunction create(Expression<?> time, ExtractField extractField) {
        return new ExtractFunction(time, extractField);
    }

    @Override
    public Double eval(Row row, ExpressionEvalContext context) {
        Object object = operand.eval(row, context);

        if (object == null) {
            return null;
        }

        try {
            return DateTimeUtils.extractField(object, extractField);
        } catch (IllegalArgumentException e) {
            throw QueryException.error(SqlErrorCode.DATA_EXCEPTION, e.getMessage(), e);
        }
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.DOUBLE;
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.EXPRESSION_EXTRACT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);

        out.writeString(extractField.name());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);

        extractField = ExtractField.valueOf(in.readString());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        ExtractFunction that = (ExtractFunction) o;
        return extractField == that.extractField;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), extractField);
    }
}
