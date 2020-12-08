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
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LikeFunctionTest extends SqlTestSupport {

    private static final ConstantExpression<?> CONST_1 = ConstantExpression.create("1", VARCHAR);
    private static final ConstantExpression<?> CONST_2 = ConstantExpression.create("2", VARCHAR);
    private static final ConstantExpression<?> CONST_3 = ConstantExpression.create("3", VARCHAR);
    private static final ConstantExpression<?> CONST_OTHER = ConstantExpression.create("100", VARCHAR);

    @Test
    public void testEquals() {
        LikeFunction function = LikeFunction.create(CONST_1, CONST_2, CONST_3);

        checkEquals(function, LikeFunction.create(CONST_1, CONST_2, CONST_3), true);
        checkEquals(function, LikeFunction.create(CONST_OTHER, CONST_2, CONST_3), false);
        checkEquals(function, LikeFunction.create(CONST_1, CONST_OTHER, CONST_3), false);
        checkEquals(function, LikeFunction.create(CONST_1, CONST_2, CONST_OTHER), false);
    }

    @Test
    public void testSerialization() {
        LikeFunction original = LikeFunction.create(CONST_1, CONST_2, CONST_3);
        LikeFunction restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_LIKE);

        checkEquals(original, restored, true);
    }
}
