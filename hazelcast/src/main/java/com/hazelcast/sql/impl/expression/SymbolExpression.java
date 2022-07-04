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

package com.hazelcast.sql.impl.expression;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

/**
 * Surrogate expression that is used to pass literal symbols during conversion.
 * <p>
 * For example, for the expression {@code TRIM(LEADING FROM field)} the first operand is Calcite's symbol "LEADING".
 * <p>
 * It is not meant to be serialized or evaluated.
 */
public final class SymbolExpression implements Expression<Object>, IdentifiedDataSerializable {

    private Object symbol;

    public SymbolExpression() {
        // No-op
    }

    private SymbolExpression(Object symbol) {
        this.symbol = symbol;
    }

    public static SymbolExpression create(Object symbol) {
        return new SymbolExpression(symbol);
    }

    @SuppressWarnings("unchecked")
    public <T> T getSymbol() {
        return (T) symbol;
    }

    @Override
    public Object eval(Row row, ExpressionEvalContext context) {
        throw unsupported();
    }

    @Override
    public QueryDataType getType() {
        throw unsupported();
    }

    @Override
    public int getFactoryId() {
        throw unsupported();
    }

    @Override
    public int getClassId() {
        throw unsupported();
    }

    @Override
    public void writeData(ObjectDataOutput out) {
        throw unsupported();
    }

    @Override
    public void readData(ObjectDataInput in) {
        throw unsupported();
    }

    private static UnsupportedOperationException unsupported() {
        throw new UnsupportedOperationException("Should not be called");
    }
}
