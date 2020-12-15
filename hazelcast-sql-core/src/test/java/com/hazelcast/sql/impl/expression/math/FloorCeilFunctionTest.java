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

package com.hazelcast.sql.impl.expression.math;

import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.sql.impl.type.QueryDataType.BIGINT;
import static com.hazelcast.sql.impl.type.QueryDataType.DECIMAL;
import static com.hazelcast.sql.impl.type.QueryDataType.DECIMAL_BIG_INTEGER;
import static com.hazelcast.sql.impl.type.QueryDataType.DOUBLE;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static com.hazelcast.sql.impl.type.QueryDataType.REAL;
import static com.hazelcast.sql.impl.type.QueryDataType.SMALLINT;
import static com.hazelcast.sql.impl.type.QueryDataType.TINYINT;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class FloorCeilFunctionTest extends SqlTestSupport {
    @Test
    public void testEquals() {
        Expression<?> function = FloorCeilFunction.create(ConstantExpression.create(1, DECIMAL), DECIMAL, true);

        checkEquals(function, FloorCeilFunction.create(ConstantExpression.create(1, DECIMAL), DECIMAL, true), true);
        checkEquals(function, FloorCeilFunction.create(ConstantExpression.create(2, DECIMAL), DECIMAL, true), false);
        checkEquals(function, FloorCeilFunction.create(ConstantExpression.create(1, DECIMAL), DECIMAL, false), false);
    }

    @Test
    public void testSerialization() {
        Expression<?> original = FloorCeilFunction.create(ConstantExpression.create(1, DECIMAL), DECIMAL, true);
        Expression<?> restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_FLOOR_CEIL);

        checkEquals(original, restored, true);
    }

    @Test
    public void testSimplification() {
        checkSimplified(FloorCeilFunction.create(ConstantExpression.create(null, TINYINT), TINYINT, true));
        checkSimplified(FloorCeilFunction.create(ConstantExpression.create(null, SMALLINT), SMALLINT, true));
        checkSimplified(FloorCeilFunction.create(ConstantExpression.create(null, INT), INT, true));
        checkSimplified(FloorCeilFunction.create(ConstantExpression.create(null, BIGINT), BIGINT, true));

        checkNotSimplified(FloorCeilFunction.create(ConstantExpression.create(null, DECIMAL_BIG_INTEGER), DECIMAL_BIG_INTEGER, true));
        checkNotSimplified(FloorCeilFunction.create(ConstantExpression.create(null, DECIMAL), DECIMAL, true));

        checkNotSimplified(FloorCeilFunction.create(ConstantExpression.create(null, REAL), REAL, true));
        checkNotSimplified(FloorCeilFunction.create(ConstantExpression.create(null, DOUBLE), DOUBLE, true));

        checkNotSimplified(FloorCeilFunction.create(ConstantExpression.create(null, TINYINT), DECIMAL, true));
    }

    private void checkNotSimplified(Expression<?> expression) {
        assertEquals(FloorCeilFunction.class, expression.getClass());
    }

    private void checkSimplified(Expression<?> expression) {
        assertEquals(ConstantExpression.class, expression.getClass());
    }
}
