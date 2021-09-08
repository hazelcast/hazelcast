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

package com.hazelcast.jet.sql.impl.expression.string;

import com.hazelcast.jet.sql.impl.support.expressions.ExpressionValue.CharacterVal;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionValue.StringVal;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.string.UpperFunction;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class UpperFunctionIntegrationTest extends StringFunctionIntegrationTestSupport {
    @Override
    protected String functionName() {
        return "UPPER";
    }

    @Override
    protected SqlColumnType resultType() {
        return SqlColumnType.VARCHAR;
    }

    @Override
    protected void checkSupportedColumns() {
        checkColumn(new CharacterVal(), null);
        checkColumn(new StringVal(), null);

        checkColumn(new CharacterVal().field1('a'), "A");
        checkColumn(new CharacterVal().field1('A'), "A");

        checkColumn(new StringVal().field1(""), "");
        checkColumn(new StringVal().field1("a"), "A");
        checkColumn(new StringVal().field1("A"), "A");
        checkColumn(new StringVal().field1("abcde"), "ABCDE");
        checkColumn(new StringVal().field1("AbCdE"), "ABCDE");
        checkColumn(new StringVal().field1("ABCDE"), "ABCDE");
    }

    @Override
    protected void checkSupportedLiterals() {
        checkLiteral("null", null);

        checkLiteral("''", "");

        checkLiteral("'a'", "A");
        checkLiteral("'A'", "A");

        checkLiteral("'abcde'", "ABCDE");
        checkLiteral("'AbCdE'", "ABCDE");
        checkLiteral("'ABCDE'", "ABCDE");
    }

    @Override
    protected void checkSupportedParameters() {
        checkParameter(null, null);

        checkParameter('a', "A");
        checkParameter('A', "A");

        checkParameter("", "");
        checkParameter("a", "A");
        checkParameter("A", "A");
        checkParameter("abcde", "ABCDE");
        checkParameter("AbCdE", "ABCDE");
        checkParameter("ABCDE", "ABCDE");
    }

    @Test
    public void testEquals() {
        UpperFunction function = UpperFunction.create(ConstantExpression.create("1", VARCHAR));

        checkEquals(function, UpperFunction.create(ConstantExpression.create("1", VARCHAR)), true);
        checkEquals(function, UpperFunction.create(ConstantExpression.create("2", VARCHAR)), false);
    }

    @Test
    public void testSerialization() {
        UpperFunction original = UpperFunction.create(ConstantExpression.create("1", VARCHAR));
        UpperFunction restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_UPPER);

        checkEquals(original, restored, true);
    }
}
