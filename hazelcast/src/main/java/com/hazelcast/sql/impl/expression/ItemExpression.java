/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;

import java.lang.reflect.Array;
import java.util.List;

/**
 * An expression to get an item from a collection.
 */
public class ItemExpression<T> extends BiCallExpressionWithType<T> {
    /** Type of container. */
    private transient ContainerType containerType;

    public ItemExpression() {
        // No-op.
    }

    public ItemExpression(Expression operand1, Expression operand2) {
        super(operand1, operand2);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T eval(QueryContext ctx, Row row) {
        Object container = operand1.eval(ctx, row);

        if (container == null) {
            return null;
        } else if (containerType == null) {
            containerType = resolveContainerType(container);
        }

        int index = getIndex(ctx, row);

        Object res;

        switch (containerType) {
            case ARRAY:
                res = Array.get(container, index);

                break;

            case LIST:
                res = ((List) container).get(index);

                break;

            default:
                throw new HazelcastSqlException(-1, "Unsupported container type: " + container.getClass());
        }

        if (res != null && resType == null) {
            resType = DataType.resolveType(res);
        }

        return (T) res;
    }

    private int getIndex(QueryContext ctx, Row row) {
        Object indexValue = operand2.eval(ctx, row);

        if (indexValue == null) {
            throw new HazelcastSqlException(-1, "Index cannot be null.");
        }

        return operand2.getType().getConverter().asInt(indexValue);
    }

    private static ContainerType resolveContainerType(Object container) {
        if (container instanceof List) {
            return ContainerType.LIST;
        } else if (container.getClass().isArray()) {
            return ContainerType.ARRAY;
        } else {
            throw new HazelcastSqlException(-1, "Unsupported container type: " + container.getClass());
        }
    }

    @Override
    public int operator() {
        return CallOperator.ITEM;
    }

    private enum ContainerType {
        ARRAY,
        LIST
    }
}
