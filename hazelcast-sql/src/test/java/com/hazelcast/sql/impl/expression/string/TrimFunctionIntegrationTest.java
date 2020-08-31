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

import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.expression.SqlExpressionIntegrationTestSupport;
import com.hazelcast.sql.support.expressions.ExpressionValue;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TrimFunctionIntegrationTest extends SqlExpressionIntegrationTestSupport {
    /**
     * Test TRIM usage with custom characters.
     * <p>
     * We assume that behavior of the input operand is tested elsewhere.
     */
    @Test
    public void test_trim_3_arg() {
        // Test normal behavior
        put(1);

        // Spaces
        checkValueInternal("SELECT TRIM(LEADING ' ' FROM '  abc  ') FROM map", SqlColumnType.VARCHAR, "abc  ");
        checkValueInternal("SELECT TRIM(TRAILING ' ' FROM '  abc  ') FROM map", SqlColumnType.VARCHAR, "  abc");
        checkValueInternal("SELECT TRIM(BOTH ' ' FROM '  abc  ') FROM map", SqlColumnType.VARCHAR, "abc");
        checkValueInternal("SELECT TRIM(' ' FROM '  abc  ') FROM map", SqlColumnType.VARCHAR, "abc");
        checkValueInternal("SELECT TRIM(' ' FROM null) FROM map", SqlColumnType.VARCHAR, null);

        // Empty operand should be no-op
        checkValueInternal("SELECT TRIM(LEADING '' FROM ' ab ') FROM map", SqlColumnType.VARCHAR, " ab ");
        checkValueInternal("SELECT TRIM(TRAILING '' FROM ' ab ') FROM map", SqlColumnType.VARCHAR, " ab ");
        checkValueInternal("SELECT TRIM(BOTH '' FROM ' ab ') FROM map", SqlColumnType.VARCHAR, " ab ");

        // Cannot trim anything because target values are "protected" with spaces
        checkValueInternal("SELECT TRIM(LEADING 'ab' FROM ' ab ') FROM map", SqlColumnType.VARCHAR, " ab ");
        checkValueInternal("SELECT TRIM(TRAILING 'ab' FROM ' ab ') FROM map", SqlColumnType.VARCHAR, " ab ");
        checkValueInternal("SELECT TRIM(BOTH 'ab' FROM ' ab ') FROM map", SqlColumnType.VARCHAR, " ab ");

        // Trim with custom characters
        checkValueInternal("SELECT TRIM(LEADING 'ab' FROM 'ababab ab ababab') FROM map", SqlColumnType.VARCHAR, " ab ababab");
        checkValueInternal("SELECT TRIM(TRAILING 'ab' FROM 'ababab ab ababab') FROM map", SqlColumnType.VARCHAR, "ababab ab ");
        checkValueInternal("SELECT TRIM(BOTH 'ab' FROM 'ababab ab ababab') FROM map", SqlColumnType.VARCHAR, " ab ");

        checkValueInternal("SELECT TRIM(LEADING 'ba' FROM 'ababab ab ababab') FROM map", SqlColumnType.VARCHAR, " ab ababab");
        checkValueInternal("SELECT TRIM(TRAILING 'ba' FROM 'ababab ab ababab') FROM map", SqlColumnType.VARCHAR, "ababab ab ");
        checkValueInternal("SELECT TRIM(BOTH 'ba' FROM 'ababab ab ababab') FROM map", SqlColumnType.VARCHAR, " ab ");

        checkValueInternal("SELECT TRIM(LEADING 'a' FROM 'aa aba aa') FROM map", SqlColumnType.VARCHAR, " aba aa");
        checkValueInternal("SELECT TRIM(TRAILING 'a' FROM 'aa aba aa') FROM map", SqlColumnType.VARCHAR, "aa aba ");
        checkValueInternal("SELECT TRIM(BOTH 'a' FROM 'aa aba aa') FROM map", SqlColumnType.VARCHAR, " aba ");

        checkValueInternal("SELECT TRIM(LEADING 'aa' FROM 'aa aba aa') FROM map", SqlColumnType.VARCHAR, " aba aa");
        checkValueInternal("SELECT TRIM(TRAILING 'aa' FROM 'aa aba aa') FROM map", SqlColumnType.VARCHAR, "aa aba ");
        checkValueInternal("SELECT TRIM(BOTH 'aa' FROM 'aa aba aa') FROM map", SqlColumnType.VARCHAR, " aba ");

        // Test column
        put(new ExpressionValue.StringVal());
        checkValueInternal("SELECT TRIM(LEADING field1 FROM 'aa aba aa') FROM map", SqlColumnType.VARCHAR, null);
        checkValueInternal("SELECT TRIM(TRAILING field1 FROM 'aa aba aa') FROM map", SqlColumnType.VARCHAR, null);
        checkValueInternal("SELECT TRIM(BOTH field1 FROM 'aa aba aa') FROM map", SqlColumnType.VARCHAR, null);

        put(new ExpressionValue.StringVal().field1("a"));
        checkValueInternal("SELECT TRIM(LEADING field1 FROM 'aa aba aa') FROM map", SqlColumnType.VARCHAR, " aba aa");
        checkValueInternal("SELECT TRIM(TRAILING field1 FROM 'aa aba aa') FROM map", SqlColumnType.VARCHAR, "aa aba ");
        checkValueInternal("SELECT TRIM(BOTH field1 FROM 'aa aba aa') FROM map", SqlColumnType.VARCHAR, " aba ");

        put(new ExpressionValue.CharacterVal().field1('a'));
        checkValueInternal("SELECT TRIM(LEADING field1 FROM 'aa aba aa') FROM map", SqlColumnType.VARCHAR, " aba aa");
        checkValueInternal("SELECT TRIM(TRAILING field1 FROM 'aa aba aa') FROM map", SqlColumnType.VARCHAR, "aa aba ");
        checkValueInternal("SELECT TRIM(BOTH field1 FROM 'aa aba aa') FROM map", SqlColumnType.VARCHAR, " aba ");

        put(new ExpressionValue.BigDecimalVal().field1(BigDecimal.ONE));
        checkValueInternal("SELECT TRIM(LEADING field1 FROM '11 121 11') FROM map", SqlColumnType.VARCHAR, " 121 11");
        checkValueInternal("SELECT TRIM(TRAILING field1 FROM '11 121 11') FROM map", SqlColumnType.VARCHAR, "11 121 ");
        checkValueInternal("SELECT TRIM(BOTH field1 FROM '11 121 11') FROM map", SqlColumnType.VARCHAR, " 121 ");

        // Test parameter
        put(1);

        checkValueInternal("SELECT TRIM(LEADING ? FROM 'aa ab aa') FROM map", SqlColumnType.VARCHAR, null, new Object[] { null });
        checkValueInternal("SELECT TRIM(TRAILING ? FROM 'aa ab aa') FROM map", SqlColumnType.VARCHAR, null, new Object[] { null });
        checkValueInternal("SELECT TRIM(BOTH ? FROM 'aa ab aa') FROM map", SqlColumnType.VARCHAR, null, new Object[] { null });

        checkValueInternal("SELECT TRIM(LEADING ? FROM 'aa ab aa') FROM map", SqlColumnType.VARCHAR, " ab aa", "a");
        checkValueInternal("SELECT TRIM(TRAILING ? FROM 'aa ab aa') FROM map", SqlColumnType.VARCHAR, "aa ab ", "a");
        checkValueInternal("SELECT TRIM(BOTH ? FROM 'aa ab aa') FROM map", SqlColumnType.VARCHAR, " ab ", "a");

        checkValueInternal("SELECT TRIM(LEADING ? FROM 'aa ab aa') FROM map", SqlColumnType.VARCHAR, " ab aa", 'a');
        checkValueInternal("SELECT TRIM(TRAILING ? FROM 'aa ab aa') FROM map", SqlColumnType.VARCHAR, "aa ab ", 'a');
        checkValueInternal("SELECT TRIM(BOTH ? FROM 'aa ab aa') FROM map", SqlColumnType.VARCHAR, " ab ", 'a');

        checkFailureInternal("SELECT TRIM(LEADING ? FROM 'aa ab aa') FROM map", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from INTEGER to VARCHAR", 1);
        checkFailureInternal("SELECT TRIM(TRAILING ? FROM 'aa ab aa') FROM map", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from INTEGER to VARCHAR", 1);
        checkFailureInternal("SELECT TRIM(BOTH ? FROM 'aa ab aa') FROM map", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from INTEGER to VARCHAR", 1);

        checkValueInternal("SELECT TRIM(LEADING ? FROM ?) FROM map", SqlColumnType.VARCHAR, " aba aa", "a", "aa aba aa");
        checkValueInternal("SELECT TRIM(TRAILING ? FROM ?) FROM map", SqlColumnType.VARCHAR, "aa aba ", "a", "aa aba aa");
        checkValueInternal("SELECT TRIM(BOTH ? FROM ?) FROM map", SqlColumnType.VARCHAR, " aba ", "a", "aa aba aa");

        // Test literals
        checkValueInternal("SELECT TRIM(LEADING null FROM '11 121 11') FROM map", SqlColumnType.VARCHAR, null);
        checkValueInternal("SELECT TRIM(TRAILING null FROM '11 121 11') FROM map", SqlColumnType.VARCHAR, null);
        checkValueInternal("SELECT TRIM(BOTH null FROM '11 121 11') FROM map", SqlColumnType.VARCHAR, null);

        checkValueInternal("SELECT TRIM(LEADING 1 FROM '11 121 11') FROM map", SqlColumnType.VARCHAR, " 121 11");
        checkValueInternal("SELECT TRIM(TRAILING 1 FROM '11 121 11') FROM map", SqlColumnType.VARCHAR, "11 121 ");
        checkValueInternal("SELECT TRIM(BOTH 1 FROM '11 121 11') FROM map", SqlColumnType.VARCHAR, " 121 ");
    }

    /**
     * Test typical TRIM usage when only spaces are removed
     */
    @Test
    public void test_trim_2_arg() {
        // Columns
        put("");
        checkValueInternal("SELECT TRIM(LEADING this) FROM map", SqlColumnType.VARCHAR, "");
        checkValueInternal("SELECT TRIM(TRAILING this) FROM map", SqlColumnType.VARCHAR, "");
        checkValueInternal("SELECT TRIM(BOTH this) FROM map", SqlColumnType.VARCHAR, "");

        put("abc");
        checkValueInternal("SELECT TRIM(LEADING this) FROM map", SqlColumnType.VARCHAR, "abc");
        checkValueInternal("SELECT TRIM(TRAILING this) FROM map", SqlColumnType.VARCHAR, "abc");
        checkValueInternal("SELECT TRIM(BOTH this) FROM map", SqlColumnType.VARCHAR, "abc");

        put(" abc");
        checkValueInternal("SELECT TRIM(LEADING this) FROM map", SqlColumnType.VARCHAR, "abc");
        checkValueInternal("SELECT TRIM(TRAILING this) FROM map", SqlColumnType.VARCHAR, " abc");
        checkValueInternal("SELECT TRIM(BOTH this) FROM map", SqlColumnType.VARCHAR, "abc");

        put("  abc");
        checkValueInternal("SELECT TRIM(LEADING this) FROM map", SqlColumnType.VARCHAR, "abc");
        checkValueInternal("SELECT TRIM(TRAILING this) FROM map", SqlColumnType.VARCHAR, "  abc");
        checkValueInternal("SELECT TRIM(BOTH this) FROM map", SqlColumnType.VARCHAR, "abc");

        put("abc ");
        checkValueInternal("SELECT TRIM(LEADING this) FROM map", SqlColumnType.VARCHAR, "abc ");
        checkValueInternal("SELECT TRIM(TRAILING this) FROM map", SqlColumnType.VARCHAR, "abc");
        checkValueInternal("SELECT TRIM(BOTH this) FROM map", SqlColumnType.VARCHAR, "abc");

        put("abc  ");
        checkValueInternal("SELECT TRIM(LEADING this) FROM map", SqlColumnType.VARCHAR, "abc  ");
        checkValueInternal("SELECT TRIM(TRAILING this) FROM map", SqlColumnType.VARCHAR, "abc");
        checkValueInternal("SELECT TRIM(BOTH this) FROM map", SqlColumnType.VARCHAR, "abc");

        put(" _ abc _ ");
        checkValueInternal("SELECT TRIM(LEADING this) FROM map", SqlColumnType.VARCHAR, "_ abc _ ");
        checkValueInternal("SELECT TRIM(TRAILING this) FROM map", SqlColumnType.VARCHAR, " _ abc _");
        checkValueInternal("SELECT TRIM(BOTH this) FROM map", SqlColumnType.VARCHAR, "_ abc _");

        put(new ExpressionValue.StringVal());
        checkValueInternal("SELECT TRIM(LEADING field1) FROM map", SqlColumnType.VARCHAR, null);
        checkValueInternal("SELECT TRIM(TRAILING field1) FROM map", SqlColumnType.VARCHAR, null);
        checkValueInternal("SELECT TRIM(BOTH field1) FROM map", SqlColumnType.VARCHAR, null);

        // Only one test for numeric column because we are going to restrict them anyway
        put(BigDecimal.ONE);
        checkValueInternal("SELECT TRIM(LEADING this) FROM map", SqlColumnType.VARCHAR, "1");
        checkValueInternal("SELECT TRIM(TRAILING this) FROM map", SqlColumnType.VARCHAR, "1");
        checkValueInternal("SELECT TRIM(BOTH this) FROM map", SqlColumnType.VARCHAR, "1");

        // Parameters
        put(1);
        checkValueInternal("SELECT TRIM(LEADING ?) FROM map", SqlColumnType.VARCHAR, null, new Object[] { null });
        checkValueInternal("SELECT TRIM(TRAILING ?) FROM map", SqlColumnType.VARCHAR, null, new Object[] { null });
        checkValueInternal("SELECT TRIM(BOTH ?) FROM map", SqlColumnType.VARCHAR, null, new Object[] { null });

        checkValueInternal("SELECT TRIM(LEADING ?) FROM map", SqlColumnType.VARCHAR, "abc ", " abc ");
        checkValueInternal("SELECT TRIM(TRAILING ?) FROM map", SqlColumnType.VARCHAR, " abc", " abc ");
        checkValueInternal("SELECT TRIM(BOTH ?) FROM map", SqlColumnType.VARCHAR, "abc", " abc ");

        checkValueInternal("SELECT TRIM(LEADING ?) FROM map", SqlColumnType.VARCHAR, "a", 'a');
        checkValueInternal("SELECT TRIM(TRAILING ?) FROM map", SqlColumnType.VARCHAR, "a", 'a');
        checkValueInternal("SELECT TRIM(BOTH ?) FROM map", SqlColumnType.VARCHAR, "a", 'a');

        checkFailureInternal("SELECT TRIM(LEADING ?) FROM map", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from INTEGER to VARCHAR", 1);
        checkFailureInternal("SELECT TRIM(TRAILING ?) FROM map", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from INTEGER to VARCHAR", 1);
        checkFailureInternal("SELECT TRIM(BOTH ?) FROM map", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from INTEGER to VARCHAR", 1);

        // Literals
        checkValueInternal("SELECT TRIM(LEADING ' abc ') FROM map", SqlColumnType.VARCHAR, "abc ");
        checkValueInternal("SELECT TRIM(TRAILING ' abc ') FROM map", SqlColumnType.VARCHAR, " abc");
        checkValueInternal("SELECT TRIM(BOTH ' abc ') FROM map", SqlColumnType.VARCHAR, "abc");

        checkValueInternal("SELECT TRIM(LEADING null) FROM map", SqlColumnType.VARCHAR, null);
        checkValueInternal("SELECT TRIM(TRAILING null) FROM map", SqlColumnType.VARCHAR, null);
        checkValueInternal("SELECT TRIM(BOTH null) FROM map", SqlColumnType.VARCHAR, null);

        checkValueInternal("SELECT TRIM(LEADING true) FROM map", SqlColumnType.VARCHAR, "true");
        checkValueInternal("SELECT TRIM(TRAILING true) FROM map", SqlColumnType.VARCHAR, "true");
        checkValueInternal("SELECT TRIM(BOTH true) FROM map", SqlColumnType.VARCHAR, "true");

        checkValueInternal("SELECT TRIM(LEADING 1) FROM map", SqlColumnType.VARCHAR, "1");
        checkValueInternal("SELECT TRIM(TRAILING 1) FROM map", SqlColumnType.VARCHAR, "1");
        checkValueInternal("SELECT TRIM(BOTH 1) FROM map", SqlColumnType.VARCHAR, "1");

        checkValueInternal("SELECT TRIM(LEADING 1.1) FROM map", SqlColumnType.VARCHAR, "1.1");
        checkValueInternal("SELECT TRIM(TRAILING 1.1) FROM map", SqlColumnType.VARCHAR, "1.1");
        checkValueInternal("SELECT TRIM(BOTH 1.1) FROM map", SqlColumnType.VARCHAR, "1.1");

        checkValueInternal("SELECT TRIM(LEADING 1.1E2) FROM map", SqlColumnType.VARCHAR, "110.0");
        checkValueInternal("SELECT TRIM(TRAILING 1.1E2) FROM map", SqlColumnType.VARCHAR, "110.0");
        checkValueInternal("SELECT TRIM(BOTH 1.1E2) FROM map", SqlColumnType.VARCHAR, "110.0");
    }

    @Test
    public void test_trim_1_arg() {
        // Columns
        put("abc");
        checkValueInternal("SELECT TRIM(this) FROM map", SqlColumnType.VARCHAR, "abc");

        put(" abc");
        checkValueInternal("SELECT TRIM(this) FROM map", SqlColumnType.VARCHAR, "abc");

        put("  abc");
        checkValueInternal("SELECT TRIM(this) FROM map", SqlColumnType.VARCHAR, "abc");

        put("abc ");
        checkValueInternal("SELECT TRIM(this) FROM map", SqlColumnType.VARCHAR, "abc");

        put("abc  ");
        checkValueInternal("SELECT TRIM(this) FROM map", SqlColumnType.VARCHAR, "abc");

        put(" _ abc _ ");
        checkValueInternal("SELECT TRIM(this) FROM map", SqlColumnType.VARCHAR, "_ abc _");

        put(new ExpressionValue.StringVal());
        checkValueInternal("SELECT TRIM(field1) FROM map", SqlColumnType.VARCHAR, null);

        // Only one test for numeric column because we are going to restrict them anyway
        put(BigDecimal.ONE);
        checkValueInternal("SELECT TRIM(this) FROM map", SqlColumnType.VARCHAR, "1");

        // Parameters
        put(1);
        checkValueInternal("SELECT TRIM(?) FROM map", SqlColumnType.VARCHAR, null, new Object[] { null });
        checkValueInternal("SELECT TRIM(?) FROM map", SqlColumnType.VARCHAR, "abc", " abc ");
        checkValueInternal("SELECT TRIM(?) FROM map", SqlColumnType.VARCHAR, "a", 'a');

        checkFailureInternal("SELECT TRIM(?) FROM map", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from INTEGER to VARCHAR", 1);

        // Literals
        checkValueInternal("SELECT TRIM(' abc ') FROM map", SqlColumnType.VARCHAR, "abc");
        checkValueInternal("SELECT TRIM(null) FROM map", SqlColumnType.VARCHAR, null);
        checkValueInternal("SELECT TRIM(true) FROM map", SqlColumnType.VARCHAR, "true");
        checkValueInternal("SELECT TRIM(1) FROM map", SqlColumnType.VARCHAR, "1");
        checkValueInternal("SELECT TRIM(1.1) FROM map", SqlColumnType.VARCHAR, "1.1");
        checkValueInternal("SELECT TRIM(1.1E2) FROM map", SqlColumnType.VARCHAR, "110.0");
    }

    @Test
    public void test_ltrim() {
        // Columns
        put("abc");
        checkValueInternal("SELECT LTRIM(this) FROM map", SqlColumnType.VARCHAR, "abc");

        put(" abc");
        checkValueInternal("SELECT LTRIM(this) FROM map", SqlColumnType.VARCHAR, "abc");

        put("  abc");
        checkValueInternal("SELECT LTRIM(this) FROM map", SqlColumnType.VARCHAR, "abc");

        put("abc ");
        checkValueInternal("SELECT LTRIM(this) FROM map", SqlColumnType.VARCHAR, "abc ");

        put("abc  ");
        checkValueInternal("SELECT LTRIM(this) FROM map", SqlColumnType.VARCHAR, "abc  ");

        put(" _ abc _ ");
        checkValueInternal("SELECT LTRIM(this) FROM map", SqlColumnType.VARCHAR, "_ abc _ ");

        put(new ExpressionValue.StringVal());
        checkValueInternal("SELECT LTRIM(field1) FROM map", SqlColumnType.VARCHAR, null);

        // Only one test for numeric column because we are going to restrict them anyway
        put(BigDecimal.ONE);
        checkValueInternal("SELECT LTRIM(this) FROM map", SqlColumnType.VARCHAR, "1");

        // Parameters
        put(1);
        checkValueInternal("SELECT LTRIM(?) FROM map", SqlColumnType.VARCHAR, null, new Object[] { null });
        checkValueInternal("SELECT LTRIM(?) FROM map", SqlColumnType.VARCHAR, "abc ", " abc ");
        checkValueInternal("SELECT LTRIM(?) FROM map", SqlColumnType.VARCHAR, "a", 'a');

        checkFailureInternal("SELECT LTRIM(?) FROM map", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from INTEGER to VARCHAR", 1);

        // Literals
        checkValueInternal("SELECT LTRIM(' abc ') FROM map", SqlColumnType.VARCHAR, "abc ");
        checkValueInternal("SELECT LTRIM(null) FROM map", SqlColumnType.VARCHAR, null);
        checkValueInternal("SELECT LTRIM(true) FROM map", SqlColumnType.VARCHAR, "true");
        checkValueInternal("SELECT LTRIM(1) FROM map", SqlColumnType.VARCHAR, "1");
        checkValueInternal("SELECT LTRIM(1.1) FROM map", SqlColumnType.VARCHAR, "1.1");
        checkValueInternal("SELECT LTRIM(1.1E2) FROM map", SqlColumnType.VARCHAR, "110.0");
    }

    @Test
    public void test_rtrim() {
        // Columns
        put("abc");
        checkValueInternal("SELECT RTRIM(this) FROM map", SqlColumnType.VARCHAR, "abc");

        put(" abc");
        checkValueInternal("SELECT RTRIM(this) FROM map", SqlColumnType.VARCHAR, " abc");

        put("  abc");
        checkValueInternal("SELECT RTRIM(this) FROM map", SqlColumnType.VARCHAR, "  abc");

        put("abc ");
        checkValueInternal("SELECT RTRIM(this) FROM map", SqlColumnType.VARCHAR, "abc");

        put("abc  ");
        checkValueInternal("SELECT RTRIM(this) FROM map", SqlColumnType.VARCHAR, "abc");

        put(" _ abc _ ");
        checkValueInternal("SELECT RTRIM(this) FROM map", SqlColumnType.VARCHAR, " _ abc _");

        put(new ExpressionValue.StringVal());
        checkValueInternal("SELECT RTRIM(field1) FROM map", SqlColumnType.VARCHAR, null);

        // Only one test for numeric column because we are going to restrict them anyway
        put(BigDecimal.ONE);
        checkValueInternal("SELECT RTRIM(this) FROM map", SqlColumnType.VARCHAR, "1");

        // Parameters
        put(1);
        checkValueInternal("SELECT RTRIM(?) FROM map", SqlColumnType.VARCHAR, null, new Object[] { null });
        checkValueInternal("SELECT RTRIM(?) FROM map", SqlColumnType.VARCHAR, " abc", " abc ");
        checkValueInternal("SELECT RTRIM(?) FROM map", SqlColumnType.VARCHAR, "a", 'a');

        checkFailureInternal("SELECT RTRIM(?) FROM map", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from INTEGER to VARCHAR", 1);

        // Literals
        checkValueInternal("SELECT RTRIM(' abc ') FROM map", SqlColumnType.VARCHAR, " abc");
        checkValueInternal("SELECT RTRIM(null) FROM map", SqlColumnType.VARCHAR, null);
        checkValueInternal("SELECT RTRIM(true) FROM map", SqlColumnType.VARCHAR, "true");
        checkValueInternal("SELECT RTRIM(1) FROM map", SqlColumnType.VARCHAR, "1");
        checkValueInternal("SELECT RTRIM(1.1) FROM map", SqlColumnType.VARCHAR, "1.1");
        checkValueInternal("SELECT RTRIM(1.1E2) FROM map", SqlColumnType.VARCHAR, "110.0");
    }

    @Test
    public void test_btrim() {
        // Columns
        put("abc");
        checkValueInternal("SELECT BTRIM(this) FROM map", SqlColumnType.VARCHAR, "abc");

        put(" abc");
        checkValueInternal("SELECT BTRIM(this) FROM map", SqlColumnType.VARCHAR, "abc");

        put("  abc");
        checkValueInternal("SELECT BTRIM(this) FROM map", SqlColumnType.VARCHAR, "abc");

        put("abc ");
        checkValueInternal("SELECT BTRIM(this) FROM map", SqlColumnType.VARCHAR, "abc");

        put("abc  ");
        checkValueInternal("SELECT BTRIM(this) FROM map", SqlColumnType.VARCHAR, "abc");

        put(" _ abc _ ");
        checkValueInternal("SELECT BTRIM(this) FROM map", SqlColumnType.VARCHAR, "_ abc _");

        put(new ExpressionValue.StringVal());
        checkValueInternal("SELECT BTRIM(field1) FROM map", SqlColumnType.VARCHAR, null);

        // Only one test for numeric column because we are going to restrict them anyway
        put(BigDecimal.ONE);
        checkValueInternal("SELECT BTRIM(this) FROM map", SqlColumnType.VARCHAR, "1");

        // Parameters
        put(1);
        checkValueInternal("SELECT BTRIM(?) FROM map", SqlColumnType.VARCHAR, null, new Object[] { null });
        checkValueInternal("SELECT BTRIM(?) FROM map", SqlColumnType.VARCHAR, "abc", " abc ");
        checkValueInternal("SELECT BTRIM(?) FROM map", SqlColumnType.VARCHAR, "a", 'a');

        checkFailureInternal("SELECT BTRIM(?) FROM map", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from INTEGER to VARCHAR", 1);

        // Literals
        checkValueInternal("SELECT BTRIM(' abc ') FROM map", SqlColumnType.VARCHAR, "abc");
        checkValueInternal("SELECT BTRIM(null) FROM map", SqlColumnType.VARCHAR, null);
        checkValueInternal("SELECT BTRIM(true) FROM map", SqlColumnType.VARCHAR, "true");
        checkValueInternal("SELECT BTRIM(1) FROM map", SqlColumnType.VARCHAR, "1");
        checkValueInternal("SELECT BTRIM(1.1) FROM map", SqlColumnType.VARCHAR, "1.1");
        checkValueInternal("SELECT BTRIM(1.1E2) FROM map", SqlColumnType.VARCHAR, "110.0");
    }
}
