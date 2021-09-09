/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.expression.math;

import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.math.RoundTruncateFunction;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.sql.impl.type.QueryDataType.BIGINT;
import static com.hazelcast.sql.impl.type.QueryDataType.DECIMAL;
import static com.hazelcast.sql.impl.type.QueryDataType.DOUBLE;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static com.hazelcast.sql.impl.type.QueryDataType.REAL;
import static com.hazelcast.sql.impl.type.QueryDataType.SMALLINT;
import static com.hazelcast.sql.impl.type.QueryDataType.TINYINT;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RoundTruncateFunctionTest extends SqlTestSupport {

    @Test
    public void testEquals() {
        ConstantExpression<?> const1 = ConstantExpression.create(1, INT);
        ConstantExpression<?> const2 = ConstantExpression.create(2, INT);
        ConstantExpression<?> constOther = ConstantExpression.create(3, INT);

        Expression<?> function = RoundTruncateFunction.create(const1, const2, INT, true);

        checkEquals(function, RoundTruncateFunction.create(const1, const2, INT, true), true);
        checkEquals(function, RoundTruncateFunction.create(constOther, const2, INT, true), false);
        checkEquals(function, RoundTruncateFunction.create(const1, constOther, INT, true), false);
        checkEquals(function, RoundTruncateFunction.create(const1, const2, BIGINT, true), false);
        checkEquals(function, RoundTruncateFunction.create(const1, const2, INT, false), false);
    }

    @Test
    public void testSerialization() {
        ConstantExpression<?> const1 = ConstantExpression.create(1, INT);
        ConstantExpression<?> const2 = ConstantExpression.create(2, INT);

        Expression<?> original = RoundTruncateFunction.create(const1, const2, DECIMAL, true);
        RoundTruncateFunction<?> restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_ROUND_TRUNCATE);

        checkEquals(original, restored, true);
    }

    @Test
    public void testSimplification() {
        // Without the second operand
        checkSimplified(RoundTruncateFunction.create(ConstantExpression.create(null, TINYINT), null, TINYINT, true));
        checkSimplified(RoundTruncateFunction.create(ConstantExpression.create(null, SMALLINT), null, SMALLINT, true));
        checkSimplified(RoundTruncateFunction.create(ConstantExpression.create(null, INT), null, INT, true));
        checkSimplified(RoundTruncateFunction.create(ConstantExpression.create(null, BIGINT), null, BIGINT, true));
        checkNotSimplified(RoundTruncateFunction.create(ConstantExpression.create(null, DECIMAL), null, DECIMAL, true));
        checkNotSimplified(RoundTruncateFunction.create(ConstantExpression.create(null, REAL), null, REAL, true));
        checkNotSimplified(RoundTruncateFunction.create(ConstantExpression.create(null, DOUBLE), null, DOUBLE, true));

        // With second operand
        ConstantExpression<?> len = ConstantExpression.create(1, INT);

        checkNotSimplified(RoundTruncateFunction.create(ConstantExpression.create(null, TINYINT), len, TINYINT, true));
        checkNotSimplified(RoundTruncateFunction.create(ConstantExpression.create(null, SMALLINT), len, SMALLINT, true));
        checkNotSimplified(RoundTruncateFunction.create(ConstantExpression.create(null, INT), len, INT, true));
        checkNotSimplified(RoundTruncateFunction.create(ConstantExpression.create(null, BIGINT), len, BIGINT, true));
        checkNotSimplified(RoundTruncateFunction.create(ConstantExpression.create(null, DECIMAL), len, DECIMAL, true));
        checkNotSimplified(RoundTruncateFunction.create(ConstantExpression.create(null, REAL), len, REAL, true));
        checkNotSimplified(RoundTruncateFunction.create(ConstantExpression.create(null, DOUBLE), len, DOUBLE, true));

        checkNotSimplified(RoundTruncateFunction.create(ConstantExpression.create(null, TINYINT), len, DECIMAL, true));
        checkNotSimplified(RoundTruncateFunction.create(ConstantExpression.create(null, SMALLINT), len, DECIMAL, true));
        checkNotSimplified(RoundTruncateFunction.create(ConstantExpression.create(null, INT), len, DECIMAL, true));
        checkNotSimplified(RoundTruncateFunction.create(ConstantExpression.create(null, BIGINT), len, DECIMAL, true));
        checkNotSimplified(RoundTruncateFunction.create(ConstantExpression.create(null, DECIMAL), len, DECIMAL, true));
        checkNotSimplified(RoundTruncateFunction.create(ConstantExpression.create(null, REAL), len, DECIMAL, true));
        checkNotSimplified(RoundTruncateFunction.create(ConstantExpression.create(null, DOUBLE), len, DECIMAL, true));
        checkNotSimplified(RoundTruncateFunction.create(ConstantExpression.create(null, VARCHAR), len, DECIMAL, true));
    }

    private void checkNotSimplified(Expression<?> expression) {
        assertEquals(RoundTruncateFunction.class, expression.getClass());
    }

    private void checkSimplified(Expression<?> expression) {
        assertEquals(ConstantExpression.class, expression.getClass());
    }
}
