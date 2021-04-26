/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.support.expressions.ExpressionValue.CharacterVal;
import com.hazelcast.sql.support.expressions.ExpressionValue.StringVal;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CharLengthFunctionIntegrationTest extends StringFunctionIntegrationTestSupport {
    @Parameterized.Parameter
    public String name;

    @Parameterized.Parameters(name = "name: {0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {"CHAR_LENGTH"},
                {"CHARACTER_LENGTH"},
                {"LENGTH"}
        });
    }

    @Override
    protected String functionName() {
        return name;
    }

    @Override
    protected SqlColumnType resultType() {
        return SqlColumnType.INTEGER;
    }

    @Override
    protected void checkSupportedColumns() {
        checkColumn(new CharacterVal().field1(null), null);
        checkColumn(new CharacterVal().field1('a'), 1);

        checkColumn(new StringVal(), null);
        checkColumn(new StringVal().field1("abcde"), 5);
        checkColumn(new StringVal().field1("abcde "), 6);
    }

    @Override
    protected void checkSupportedLiterals() {
        checkLiteral("null", null);
        checkLiteral("'a'", 1);
        checkLiteral("'abcde'", 5);
        checkLiteral("'abcde '", 6);
    }

    @Override
    protected void checkSupportedParameters() {
        checkParameter(null, null);
        checkParameter('a', 1);
        checkParameter("abcde", 5);
        checkParameter("abcde ", 6);
    }

    @Test
    public void testEquals() {
        CharLengthFunction function = CharLengthFunction.create(ConstantExpression.create("1", VARCHAR));

        checkEquals(function, CharLengthFunction.create(ConstantExpression.create("1", VARCHAR)), true);
        checkEquals(function, CharLengthFunction.create(ConstantExpression.create("2", VARCHAR)), false);
    }

    @Test
    public void testSerialization() {
        CharLengthFunction original = CharLengthFunction.create(ConstantExpression.create("1", VARCHAR));
        CharLengthFunction restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_CHAR_LENGTH);

        checkEquals(original, restored, true);
    }
}
