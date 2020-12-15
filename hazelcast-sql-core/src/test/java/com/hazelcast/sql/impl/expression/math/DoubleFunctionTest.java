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
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.sql.impl.SqlTestSupport.checkEquals;
import static com.hazelcast.sql.impl.SqlTestSupport.serializeAndCheck;
import static com.hazelcast.sql.impl.expression.math.DoubleFunction.COS;
import static com.hazelcast.sql.impl.expression.math.DoubleFunction.SIN;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DoubleFunctionTest {
    @Test
    public void testEquals() {
        DoubleFunction function = DoubleFunction.create(ConstantExpression.create(1, QueryDataType.INT), COS);

        checkEquals(function, DoubleFunction.create(ConstantExpression.create(1, QueryDataType.INT), COS), true);
        checkEquals(function, DoubleFunction.create(ConstantExpression.create(2, QueryDataType.INT), COS), false);
        checkEquals(function, DoubleFunction.create(ConstantExpression.create(1, QueryDataType.INT), SIN), false);
    }

    @Test
    public void testSerialization() {
        DoubleFunction original = DoubleFunction.create(ConstantExpression.create(1, QueryDataType.INT), COS);
        DoubleFunction restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_DOUBLE);

        checkEquals(original, restored, true);
    }
}
