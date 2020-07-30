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

package com.hazelcast.sql.impl.expression.predicate;

import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.SimpleExpressionEvalContext;
import com.hazelcast.sql.impl.row.EmptyRow;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IsTruePredicateTest extends SqlTestSupport {
    @Test
    public void testEvaluation() {
        ExpressionEvalContext context = SimpleExpressionEvalContext.create();
        Row row = EmptyRow.INSTANCE;

        assertEquals(true, IsTruePredicate.create(ConstantExpression.create(true, QueryDataType.BOOLEAN)).eval(row, context));
        assertEquals(false, IsTruePredicate.create(ConstantExpression.create(false, QueryDataType.BOOLEAN)).eval(row, context));
        assertEquals(false, IsTruePredicate.create(ConstantExpression.create(null, QueryDataType.BOOLEAN)).eval(row, context));
    }

    @Test
    public void testType() {
        IsTruePredicate predicate = IsTruePredicate.create(ConstantExpression.create(true, QueryDataType.BOOLEAN));

        assertEquals(QueryDataType.BOOLEAN, predicate.getType());
    }

    @Test
    public void testEquality() {
        IsTruePredicate predicate = IsTruePredicate.create(ConstantExpression.create(true, QueryDataType.BOOLEAN));

        checkEquals(predicate, IsTruePredicate.create(ConstantExpression.create(true, QueryDataType.BOOLEAN)), true);
        checkEquals(predicate, IsTruePredicate.create(ConstantExpression.create(false, QueryDataType.BOOLEAN)), false);
    }

    @Test
    public void testSerialization() {
        IsTruePredicate original = IsTruePredicate.create(ConstantExpression.create(true, QueryDataType.BOOLEAN));
        IsTruePredicate restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_IS_TRUE);

        checkEquals(original, restored, true);
    }
}
