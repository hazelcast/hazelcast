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
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AbsFunctionTest extends SqlTestSupport {
    @Test
    public void testEquals() {
        AbsFunction<?> function = AbsFunction.create(ConstantExpression.create(1, QueryDataType.INT), QueryDataType.BIGINT);

        checkEquals(function, AbsFunction.create(ConstantExpression.create(1, QueryDataType.INT), QueryDataType.BIGINT), true);
        checkEquals(function, AbsFunction.create(ConstantExpression.create(2, QueryDataType.INT), QueryDataType.BIGINT), false);
        checkEquals(function, AbsFunction.create(ConstantExpression.create(1, QueryDataType.INT), QueryDataType.VARCHAR), false);
    }

    @Test
    public void testSerialization() {
        AbsFunction<?> original = AbsFunction.create(ConstantExpression.create(1, QueryDataType.INT), QueryDataType.BIGINT);
        AbsFunction<?> restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_ABS);

        checkEquals(original, restored, true);
    }
}
