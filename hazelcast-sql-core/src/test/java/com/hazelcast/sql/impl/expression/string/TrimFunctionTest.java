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

import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TrimFunctionTest extends SqlTestSupport {
    @Test
    public void testEquals() {
        Expression<?> input1 = constant("a");
        Expression<?> input2 = constant("b");

        Expression<?> characters1 = constant("c");
        Expression<?> characters2 = constant("d");

        boolean leading1 = true;
        boolean leading2 = false;

        boolean trailing1 = true;
        boolean trailing2 = false;

        TrimFunction function = TrimFunction.create(input1, characters1, leading1, trailing1);

        checkEquals(function, TrimFunction.create(input1, characters1, leading1, trailing1), true);
        checkEquals(function, TrimFunction.create(input2, characters1, leading1, trailing1), false);
        checkEquals(function, TrimFunction.create(input1, characters2, leading1, trailing1), false);
        checkEquals(function, TrimFunction.create(input1, characters1, leading2, trailing1), false);
        checkEquals(function, TrimFunction.create(input1, characters1, leading1, trailing2), false);
    }

    @Test
    public void testSerialization() {
        TrimFunction original = TrimFunction.create(constant("a"), constant("b"), true, true);
        TrimFunction restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_TRIM);

        checkEquals(original, restored, true);
    }

    @Test
    public void testSimplification() {
        TrimFunction function = TrimFunction.create(constant("a"), constant("b"), true, true);
        assertEquals(ConstantExpression.create("b", VARCHAR), function.getCharacters());

        function = TrimFunction.create(constant("a"), constant(" "), true, true);
        assertNull(function.getCharacters());

        function = TrimFunction.create(constant("a"), null, true, true);
        assertNull(function.getCharacters());
    }

    private ConstantExpression<?> constant(String value) {
        return ConstantExpression.create(value, VARCHAR);
    }
}
