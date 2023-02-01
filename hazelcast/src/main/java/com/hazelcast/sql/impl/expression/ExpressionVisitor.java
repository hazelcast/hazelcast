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

    R visit(AndPredicate predicate);
    R visit(OrPredicate predicate);
    R visit(ParameterExpression<?> expr);
    R visit(ConstantExpression<?> expr);
    R visit(IsTruePredicate predicate);
    R visit(IsFalsePredicate predicate);
    R visit(IsNullPredicate predicate);
    R visit(IsNotNullPredicate predicate);
    R visit(ColumnExpression<?> expr);
    R visit(ComparisonPredicate expr);



}
