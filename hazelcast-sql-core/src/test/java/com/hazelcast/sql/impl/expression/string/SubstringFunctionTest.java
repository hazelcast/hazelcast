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

package com.hazelcast.sql.impl.expression.string;

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

import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SubstringFunctionTest extends SqlTestSupport {
    @Test
    public void testEquals() {
        Expression<?> input1 = ConstantExpression.create("a", VARCHAR);
        Expression<?> input2 = ConstantExpression.create("b", VARCHAR);

        Expression<?> start1 = ConstantExpression.create(1, INT);
        Expression<?> start2 = ConstantExpression.create(2, INT);

        Expression<?> length1 = ConstantExpression.create(10, INT);
        Expression<?> length2 = ConstantExpression.create(20, INT);

        SubstringFunction function = SubstringFunction.create(input1, start1, length1);

        checkEquals(function, SubstringFunction.create(input1, start1, length1), true);
        checkEquals(function, SubstringFunction.create(input2, start1, length1), false);
        checkEquals(function, SubstringFunction.create(input1, start2, length1), false);
        checkEquals(function, SubstringFunction.create(input1, start1, length2), false);
    }

    @Test
    public void testSerialization() {
        SubstringFunction original = SubstringFunction.create(
            ConstantExpression.create("a", VARCHAR),
            ConstantExpression.create(1, INT),
            ConstantExpression.create(10, INT)
        );

        SubstringFunction restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_SUBSTRING);

        checkEquals(original, restored, true);
    }
}
