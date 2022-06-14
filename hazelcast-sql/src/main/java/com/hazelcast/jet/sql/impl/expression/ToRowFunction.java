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

package com.hazelcast.jet.sql.impl.expression;

import com.hazelcast.jet.impl.util.ReflectionUtils;
import com.hazelcast.jet.sql.impl.JetSqlSerializerHook;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.RowValue;
import com.hazelcast.sql.impl.expression.UniExpressionWithType;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ToRowFunction extends UniExpressionWithType<RowValue> implements IdentifiedDataSerializable {

    public ToRowFunction() { }

    private ToRowFunction(Expression<?> operand) {
        super(operand, QueryDataType.ROW);
    }

    public static ToRowFunction create(Expression<?> operand) {
        return new ToRowFunction(operand);
    }

    @Override
    public int getFactoryId() {
        return JetSqlSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return JetSqlSerializerHook.TO_ROW;
    }

    @Override
    public RowValue eval(final Row row, final ExpressionEvalContext context) {
        final Object object = this.operand.eval(row, context);
        final QueryDataType queryDataType = operand.getType();

        return convert(object, queryDataType, new HashSet<>());
    }

    private RowValue convert(final Object obj, final QueryDataType dataType, final Set<Integer> foundObjects) {
        foundObjects.add(System.identityHashCode(obj));

        final List<Object> fieldValues = new ArrayList<>();
        for (final QueryDataType.QueryDataTypeField field : dataType.getFields()) {
            final Object fieldValue = getFieldValue(field.getName(), obj);
            if (!field.getDataType().isCustomType() || fieldValue == null) {
                fieldValues.add(fieldValue);
                continue;
            }
            if (!foundObjects.contains(System.identityHashCode(fieldValue))) {
                    fieldValues.add(convert(fieldValue, field.getDataType(), foundObjects));
            }
        }
        return new RowValue(fieldValues);
    }

    private Object getFieldValue(String fieldName, Object obj) {
        final Method getter = ReflectionUtils.findPropertyGetter(obj.getClass(), fieldName);
        if (getter != null) {
            try {
                return getter.invoke(obj);
            } catch (IllegalAccessException | InvocationTargetException ignored) { }
        }

        final Field field = ReflectionUtils.findPropertyField(obj.getClass(), fieldName);
        if (field == null) {
            return null;
        }

        final boolean accessible = field.isAccessible();
        field.setAccessible(true);
        try {
            return field.get(obj);
        } catch (IllegalAccessException ignored) {
            return null;
        } finally {
            field.setAccessible(accessible);
        }
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.ROW;
    }
}
