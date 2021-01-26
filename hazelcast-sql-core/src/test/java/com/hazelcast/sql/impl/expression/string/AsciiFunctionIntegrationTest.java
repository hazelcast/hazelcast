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
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AsciiFunctionIntegrationTest extends StringFunctionIntegrationTestSupport {
    @Override
    protected String functionName() {
        return "ASCII";
    }

    @Override
    protected SqlColumnType resultType() {
        return SqlColumnType.INTEGER;
    }

    @Override
    protected void checkSupportedColumns() {
        checkColumn(new CharacterVal(), null);
        checkColumn(new StringVal(), null);

        checkColumn(new CharacterVal().field1('a'), codePoint('a'));
        checkColumn(new CharacterVal().field1('A'), codePoint('A'));

        checkColumn(new StringVal().field1("abc"), codePoint('a'));
        checkColumn(new StringVal().field1("ABC"), codePoint('A'));
    }

    @Override
    protected void checkSupportedLiterals() {
        checkLiteral("null", codePoint(null));

        checkLiteral("'a'", codePoint('a'));
        checkLiteral("'A'", codePoint('A'));
        checkLiteral("'abc'", codePoint('a'));
        checkLiteral("'ABC'", codePoint('A'));
    }

    @Override
    protected void checkSupportedParameters() {
        checkParameter(null, null);

        checkParameter("", 0);
        checkParameter('a', codePoint('a'));
        checkParameter('A', codePoint('A'));

        checkParameter("a", codePoint('a'));
        checkParameter("A", codePoint('A'));
        checkParameter("abc", codePoint('a'));
        checkParameter("ABC", codePoint('A'));
    }

    @Test
    public void testEquals() {
        AsciiFunction function = AsciiFunction.create(ConstantExpression.create("1", VARCHAR));

        checkEquals(function, AsciiFunction.create(ConstantExpression.create("1", VARCHAR)), true);
        checkEquals(function, AsciiFunction.create(ConstantExpression.create("2", VARCHAR)), false);
    }

    @Test
    public void testSerialization() {
        AsciiFunction original = AsciiFunction.create(ConstantExpression.create("1", VARCHAR));
        AsciiFunction restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_ASCII);

        checkEquals(original, restored, true);
    }

    private static Integer codePoint(Character value) {
        if (value == null) {
            return null;
        }

        return value.toString().codePoints().toArray()[0];
    }
}
