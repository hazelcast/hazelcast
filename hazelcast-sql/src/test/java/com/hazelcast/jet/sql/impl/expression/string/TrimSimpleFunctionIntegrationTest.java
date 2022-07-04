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

import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionValue.CharacterVal;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionValue.StringVal;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TrimSimpleFunctionIntegrationTest extends StringFunctionIntegrationTestSupport {
    @Parameterized.Parameter
    public Mode mode;

    @Parameterized.Parameters(name = "name: {0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {Mode.TRIM},
                {Mode.BTRIM},
                {Mode.LTRIM},
                {Mode.RTRIM}
        });
    }

    @Override
    protected String functionName() {
        return mode.name();
    }

    @Override
    protected SqlColumnType resultType() {
        return SqlColumnType.VARCHAR;
    }

    @Override
    protected void checkSupportedColumns() {
        checkColumn(new CharacterVal(), null);
        checkColumn(new StringVal(), null);

        checkColumn(new CharacterVal().field1('a'), "a");

        checkColumn(new StringVal().field1(""), "");
        checkColumn(new StringVal().field1("a"), "a");
        checkColumn(new StringVal().field1("abc"), trim("abc"));
        checkColumn(new StringVal().field1("abc "), trim("abc "));
        checkColumn(new StringVal().field1("abc   "), trim("abc   "));
        checkColumn(new StringVal().field1(" abc"), trim(" abc"));
        checkColumn(new StringVal().field1("   abc"), trim("   abc"));
        checkColumn(new StringVal().field1(" abc "), trim(" abc "));
        checkColumn(new StringVal().field1("   abc   "), trim("   abc   "));
    }

    @Override
    protected void checkSupportedLiterals() {
        checkLiteral("null", null);

        checkLiteral("''", "");

        checkLiteral("'a'", trim("a"));
        checkLiteral("'abc'", trim("abc"));
        checkLiteral("'abc '", trim("abc "));
        checkLiteral("'abc   '", trim("abc   "));
        checkLiteral("' abc'", trim(" abc"));
        checkLiteral("'   abc'", trim("   abc"));
        checkLiteral("' abc '", trim(" abc "));
        checkLiteral("'   abc   '", trim("   abc   "));
    }

    @Override
    protected void checkSupportedParameters() {
        checkParameter(null, null);

        checkParameter('a', trim("a"));

        checkParameter("", "");
        checkParameter("a", trim("a"));
        checkParameter("abc", trim("abc"));
        checkParameter("abc ", trim("abc "));
        checkParameter("abc   ", trim("abc   "));
        checkParameter(" abc", trim(" abc"));
        checkParameter("   abc", trim("   abc"));
        checkParameter(" abc ", trim(" abc "));
        checkParameter("   abc   ", trim("   abc   "));
    }

    private String trim(String value) {
        if (value == null) {
            return null;
        }

        if (mode.left && mode.right) {
            value = value.trim();
        } else if (mode.left) {
            while (value.startsWith(" ")) {
                value = value.substring(1);
            }
        } else {
            while (value.endsWith(" ")) {
                value = value.substring(0, value.length() - 1);
            }
        }

        return value;
    }

    private enum Mode {
        TRIM(true, true),
        BTRIM(true, true),
        LTRIM(true, false),
        RTRIM(false, true);

        private final boolean left;
        private final boolean right;

        Mode(boolean left, boolean right) {
            this.left = left;
            this.right = right;
        }
    }
}
