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
public class OrPredicateTest extends SqlTestSupport {

    // NOTE: This test class verifies only basic functionality, look for more
    // extensive tests in hazelcast-sql module.

    @Test
    public void testCreationAndEval() {
        assertTrue(or(true, false).eval(row("foo"), SimpleExpressionEvalContext.create()));
        assertFalse(or(false, false).eval(row("foo"), SimpleExpressionEvalContext.create()));
        assertNull(or(null, false).eval(row("foo"), SimpleExpressionEvalContext.create()));
    }

    @Test
    public void testEquality() {
        checkEquals(or(true, false), or(true, false), true);
        checkEquals(or(true, false), or(true, true), false);
        checkEquals(or(true, true), or(true, true, true), false);
    }

    @Test
    public void testSerialization() {
        OrPredicate original = or(true, false);
        OrPredicate restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_OR);

        checkEquals(original, restored, true);
    }

    private static OrPredicate or(Boolean... values) {
        Expression<?>[] operands = new Expression<?>[values.length];
        for (int i = 0; i < values.length; ++i) {
            operands[i] = ConstantExpression.create(values[i], BOOLEAN);
        }
        return OrPredicate.create(operands);
    }

}
