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
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.ExpressionTestBase;
import com.hazelcast.sql.impl.expression.SimpleExpressionEvalContext;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.sql.impl.expression.predicate.ComparisonMode.EQUALS;
import static com.hazelcast.sql.impl.expression.predicate.ComparisonMode.GREATER_THAN;
import static com.hazelcast.sql.impl.expression.predicate.ComparisonMode.LESS_THAN;
import static com.hazelcast.sql.impl.type.QueryDataType.BIGINT;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ComparisonPredicateTest extends ExpressionTestBase {

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testCreationAndEval() {
        assertFalse(comparison(0, 1, INT, EQUALS).eval(row("foo"), SimpleExpressionEvalContext.create()));
        assertTrue(comparison(0, 1, INT, LESS_THAN).eval(row("foo"), SimpleExpressionEvalContext.create()));
    }

    @Test
    public void testEquality() {
        checkEquals(comparison(0, 1, INT, EQUALS), comparison(0, 1, INT, EQUALS), true);
        checkEquals(comparison(0, 1, INT, EQUALS), comparison(0, 1, INT, GREATER_THAN), false);
        checkEquals(comparison(0, 1, INT, EQUALS), comparison(0, 1, BIGINT, EQUALS), false);
        checkEquals(comparison(0, 1, INT, EQUALS), comparison(1, 1, INT, EQUALS), false);
        checkEquals(comparison(0, 1, INT, EQUALS), comparison(1, 0, INT, EQUALS), false);
    }

    @Test
    public void testSerialization() {
        ComparisonPredicate original = comparison(0, 1, INT, EQUALS);
        ComparisonPredicate restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_COMPARISON);

        checkEquals(original, restored, true);
    }

    private static ComparisonPredicate comparison(Object lhs, Object rhs, QueryDataType type, ComparisonMode mode) {
        return ComparisonPredicate.create(ConstantExpression.create(lhs, type), ConstantExpression.create(rhs, type), mode);
    }

}
