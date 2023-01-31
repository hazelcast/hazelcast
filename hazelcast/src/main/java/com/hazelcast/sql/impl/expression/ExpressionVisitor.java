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

import com.hazelcast.sql.impl.expression.predicate.AndPredicate;
import com.hazelcast.sql.impl.expression.predicate.ComparisonPredicate;
import com.hazelcast.sql.impl.expression.predicate.IsFalsePredicate;
import com.hazelcast.sql.impl.expression.predicate.IsNotNullPredicate;
import com.hazelcast.sql.impl.expression.predicate.IsNullPredicate;
import com.hazelcast.sql.impl.expression.predicate.IsTruePredicate;
import com.hazelcast.sql.impl.expression.predicate.OrPredicate;

public interface ExpressionVisitor<R> {

    @SuppressWarnings("unchecked")
    default R visitGeneric(Expression<?> expr) {
        Expression<R> e = (Expression<R>) expr;
        return visit(e);
    }

    /**
     * Workaround to the fact, that we don't have (yet) accept method in the Expression.
     * TODO make proper visitor
     */
    @SuppressWarnings("checkstyle:ReturnCount")
    default R visit(Expression<R> expr) {
        if (expr instanceof AndPredicate) {
            return visit((AndPredicate) expr);
        } else if (expr instanceof OrPredicate) {
            return visit((OrPredicate) expr);
        } else if (expr instanceof ParameterExpression) {
            return visit((ParameterExpression<R>) expr);
        } else if (expr instanceof ConstantExpression) {
            return visit((ConstantExpression<R>) expr);
        } else if (expr instanceof IsTruePredicate) {
            return visit((IsTruePredicate) expr);
        } else if (expr instanceof IsFalsePredicate) {
            return visit((IsFalsePredicate) expr);
        } else if (expr instanceof IsNullPredicate) {
            return visit((IsNullPredicate) expr);
        } else if (expr instanceof IsNotNullPredicate) {
            return visit((IsNotNullPredicate) expr);
        } else if (expr instanceof ColumnExpression) {
            return visit((ColumnExpression<R>) expr);
        }  else if (expr instanceof ComparisonPredicate) {
            return visit((ComparisonPredicate) expr);
        } else {
            throw new UnsupportedOperationException("expression type " + expr.getClass() + " is not supported yet");
        }
    }

    R visit(AndPredicate predicate);
    R visit(OrPredicate predicate);
    R visit(ParameterExpression<R> expr);
    R visit(ConstantExpression<R> expr);
    R visit(IsTruePredicate predicate);
    R visit(IsFalsePredicate predicate);
    R visit(IsNullPredicate predicate);
    R visit(IsNotNullPredicate predicate);
    R visit(ColumnExpression<R> expr);
    R visit(ComparisonPredicate expr);



}
