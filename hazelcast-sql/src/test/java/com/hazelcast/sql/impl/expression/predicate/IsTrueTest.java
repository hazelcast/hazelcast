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
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.SimpleExpressionEvalContext;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IsTrueTest extends SqlTestSupport {
    @Test
    public void testCreationAndEval() {
        IsTruePredicate predicate = IsTruePredicate.create(ColumnExpression.create(0, QueryDataType.BOOLEAN));

        assertTrue(predicate.eval(row(true), SimpleExpressionEvalContext.create()));
        assertFalse(predicate.eval(row(false), SimpleExpressionEvalContext.create()));
        assertFalse(predicate.eval(row(new Object[]{null}), SimpleExpressionEvalContext.create()));
    }

    @Test
    public void testEquality() {
        ColumnExpression<?> column1 = ColumnExpression.create(1, QueryDataType.BOOLEAN);
        ColumnExpression<?> column2 = ColumnExpression.create(2, QueryDataType.BOOLEAN);

        checkEquals(IsTruePredicate.create(column1), IsTruePredicate.create(column1), true);
        checkEquals(IsTruePredicate.create(column1), IsTruePredicate.create(column2), false);
    }

    @Test
    public void testSerialization() {
        IsTruePredicate original = IsTruePredicate.create(ColumnExpression.create(1, QueryDataType.BOOLEAN));
        IsTruePredicate restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_IS_TRUE);

        checkEquals(original, restored, true);
    }
}
