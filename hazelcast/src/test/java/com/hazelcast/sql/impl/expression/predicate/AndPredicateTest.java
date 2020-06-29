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
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.SimpleExpressionEvalContext;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.sql.impl.type.QueryDataType.BOOLEAN;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AndPredicateTest extends SqlTestSupport {

    // NOTE: This test class verifies only basic functionality, look for more
    // extensive tests in hazelcast-sql module.

    @Test
    public void testCreationAndEval() {
        assertFalse(and(true, false).eval(row("foo"), SimpleExpressionEvalContext.create()));
        assertTrue(and(true, true).eval(row("foo"), SimpleExpressionEvalContext.create()));
        assertNull(and(null, true).eval(row("foo"), SimpleExpressionEvalContext.create()));
    }

    @Test
    public void testEquality() {
        checkEquals(and(true, false), and(true, false), true);
        checkEquals(and(true, false), and(true, true), false);
        checkEquals(and(true, true), and(true, true, true), false);
    }

    @Test
    public void testSerialization() {
        AndPredicate original = and(true, false);
        AndPredicate restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_AND);

        checkEquals(original, restored, true);
    }

    private static AndPredicate and(Boolean... values) {
        Expression<?>[] operands = new Expression<?>[values.length];
        for (int i = 0; i < values.length; ++i) {
            operands[i] = ConstantExpression.create(values[i], BOOLEAN);
        }
        return AndPredicate.create(operands);
    }

}
