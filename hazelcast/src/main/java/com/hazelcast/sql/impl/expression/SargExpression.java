/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.expression.predicate.SearchPredicate;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;
import java.util.Objects;

/**
 * An expression representing a Sarg literal as an operand to {@link
 * SearchPredicate}.
 */
public class SargExpression<C extends Comparable<C>> implements Expression<AbstractSarg<C>>, IdentifiedDataSerializable {

    private QueryDataType type;
    private AbstractSarg<C> sarg;

    public SargExpression() {
    }

    private SargExpression(QueryDataType type, AbstractSarg<C> sarg) {
        this.type = type;
        this.sarg = sarg;
    }

    public static SargExpression<?> create(QueryDataType type, AbstractSarg<?> searchable) {
        return new SargExpression<>(type, searchable);
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.SARG_EXPRESSION;
    }

    @Override
    public AbstractSarg<C> eval(Row row, ExpressionEvalContext context) {
        return sarg;
    }

    @Override
    public QueryDataType getType() {
        return type;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(type);
        out.writeObject(sarg);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        type = in.readObject();
        sarg = in.readObject();
    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + (sarg != null ? sarg.hashCode() : 0);
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SargExpression<?> that = (SargExpression<?>) o;

        return Objects.equals(type, that.type) && Objects.equals(sarg, that.sarg);
    }

    @Override
    public String toString() {
        return "SearchableExpression{type=" + type + ", searchable=" + sarg + '}';
    }
}
