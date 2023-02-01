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

package com.hazelcast.jet.sql.impl.connector.mongodb;

import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.ExpressionVisitor;
import com.hazelcast.sql.impl.expression.ParameterExpression;
import com.hazelcast.sql.impl.expression.predicate.AndPredicate;
import com.hazelcast.sql.impl.expression.predicate.ComparisonPredicate;
import com.hazelcast.sql.impl.expression.predicate.IsFalsePredicate;
import com.hazelcast.sql.impl.expression.predicate.IsNotNullPredicate;
import com.hazelcast.sql.impl.expression.predicate.IsNullPredicate;
import com.hazelcast.sql.impl.expression.predicate.IsTruePredicate;
import com.hazelcast.sql.impl.expression.predicate.OrPredicate;
import com.mongodb.client.model.Filters;
import org.bson.conversions.Bson;

/**
 * Visitor that converts Hazelcast {@linkplain Expression}s to Mongo expressions (filters, projections).
 */
@SuppressWarnings("unchecked")
final class ExpressionToMongoVisitor implements ExpressionVisitor<Object> {

    private final MongoTable table;
    private final ExpressionEvalContext context;

    /**
     * If true, dynamic variables will be resolved as simple strings, otherwise it will use
     * {@linkplain #context} to gather correct value.
     */
    private final boolean dryRun;

    ExpressionToMongoVisitor(MongoTable table, ExpressionEvalContext context, boolean dryRun) {
        this.table = table;
        this.context = context;
        this.dryRun = dryRun;
    }

    @Override
    public Object visit(AndPredicate predicate) {
        Bson[] filters = getFilters(predicate.operands());
        return Filters.and(filters);
    }

    @Override
    public Object visit(OrPredicate predicate) {
        Bson[] filters = getFilters(predicate.operands());
        return Filters.or(filters);
    }

    private Bson[] getFilters(Expression<?>[] operands) {
        Bson[] filters = new Bson[operands.length];
        for (int i = 0; i < operands.length; i++) {
            Expression<Object> expr = (Expression<Object>) operands[i];
            Object r = expr.accept(this);
            if (r instanceof Bson) {
                filters[i] = (Bson) r;
            } else if (r instanceof String) {
                filters[i] = Filters.eq((String) r, true);
            } else {
                throw new UnsupportedOperationException();
            }
        }
        return filters;
    }

    @Override
    public Object visit(ParameterExpression<?> expr) {
        if (dryRun) {
            return "/replaceParam:" + expr.getIndex() + "/";
        }
        Object argument = context.getArgument(expr.getIndex());
        return argument;
    }

    @Override
    public Object visit(ConstantExpression<?> expr) {
        return expr.getValue();
    }

    @Override
    public Object visit(IsTruePredicate predicate) {
        Expression<?> operand = predicate.getOperand();
        return operand.accept(this);
    }

    @Override
    public Object visit(IsFalsePredicate predicate) {
        return predicate.getOperand().accept(this);
    }

    @Override
    public Object visit(IsNullPredicate predicate) {
        return predicate.getOperand().accept(this);
    }

    @Override
    public Object visit(IsNotNullPredicate predicate) {
        return predicate.getOperand().accept(this);
    }

    @Override
    public Object visit(ColumnExpression<?> expr) {
        int index = expr.getIndex();
        MongoTableField field = table.getField(index);
        return field.externalName;
    }

    @Override
    public Object visit(ComparisonPredicate predicate) {
        Expression<?> operand1 = predicate.getOperand1();
        Expression<?> operand2 = predicate.getOperand2();
        Object o1 = operand1.accept(this);
        Object o2 = operand2.accept(this);

        switch (predicate.getMode()) {
            case EQUALS: return Filters.eq((String) o1, o2);
            case GREATER_THAN: return Filters.gt((String) o1, o2);
            case GREATER_THAN_OR_EQUAL: return Filters.gte((String) o1, o2);
            case LESS_THAN: return Filters.lt((String) o1, o2);
            case LESS_THAN_OR_EQUAL: return Filters.lte((String) o1, o2);
            default: throw new IllegalArgumentException("Mode " + predicate.getMode() + " is not supported");
        }
    }
}
