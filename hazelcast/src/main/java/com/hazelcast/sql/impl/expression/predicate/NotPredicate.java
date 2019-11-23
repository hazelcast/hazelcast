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

package com.hazelcast.sql.impl.expression.predicate;

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.expression.CallOperator;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.UniCallExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;

import java.util.Objects;

/**
 * Not predicate.
 */
public class NotPredicate extends UniCallExpression<Boolean> {
    /** Whether the operand is checked. */
    private transient boolean operandChecked;

    public NotPredicate() {
        // No-op.
    }

    public NotPredicate(Expression operand) {
        super(operand);
    }

    @Override
    public int operator() {
        return CallOperator.NOT;
    }

    @Override
    public Boolean eval(QueryContext ctx, Row row) {
        Object operandValue = operand.eval(ctx, row);

        if (operandValue == null) {
            return null;
        }

        if (!operandChecked) {
            if (operand.getType() != DataType.BIT) {
                throw new HazelcastSqlException(-1, "Operand is not BIT.");
            }

            operandChecked = true;
        }

        Boolean operandValue0 = (Boolean) operandValue;

        return !operandValue0;
    }

    @Override
    public DataType getType() {
        return null;
    }

    @Override
    public int hashCode() {
        return operand.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        NotPredicate that = (NotPredicate) o;

        return Objects.equals(operand, that.operand);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{operand=" + operand + '}';
    }
}
