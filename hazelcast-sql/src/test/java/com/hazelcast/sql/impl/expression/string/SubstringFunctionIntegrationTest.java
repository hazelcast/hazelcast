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
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.expression.SqlExpressionIntegrationTestSupport;
import com.hazelcast.sql.support.expressions.ExpressionValue;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.math.BigInteger;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SubstringFunctionIntegrationTest extends SqlExpressionIntegrationTestSupport {
    @Test
    public void test_input() {
        // String column
        put("abcde");
        checkValueInternal("SELECT SUBSTRING(this FROM null) FROM map", SqlColumnType.VARCHAR, "abcde");put("abcde");
        checkValueInternal("SELECT SUBSTRING(this FROM 1) FROM map", SqlColumnType.VARCHAR, "abcde");
        checkValueInternal("SELECT SUBSTRING(this FROM 2) FROM map", SqlColumnType.VARCHAR, "bcde");
        checkValueInternal("SELECT SUBSTRING(this FROM 5) FROM map", SqlColumnType.VARCHAR, "e");
        checkValueInternal("SELECT SUBSTRING(this FROM 6) FROM map", SqlColumnType.VARCHAR, "");
        checkFailureInternal("SELECT SUBSTRING(this FROM 0) FROM map", SqlErrorCode.DATA_EXCEPTION, "SUBSTRING \"start\" operand must be positive");
        checkFailureInternal("SELECT SUBSTRING(this FROM -1) FROM map", SqlErrorCode.DATA_EXCEPTION, "SUBSTRING \"start\" operand must be positive");
        checkValueInternal("SELECT SUBSTRING(this FROM 1 FOR 0) FROM map", SqlColumnType.VARCHAR, "");
        checkValueInternal("SELECT SUBSTRING(this FROM 1 FOR 1) FROM map", SqlColumnType.VARCHAR, "a");
        checkValueInternal("SELECT SUBSTRING(this FROM 1 FOR 2) FROM map", SqlColumnType.VARCHAR, "ab");
        checkValueInternal("SELECT SUBSTRING(this FROM 1 FOR 5) FROM map", SqlColumnType.VARCHAR, "abcde");
        checkValueInternal("SELECT SUBSTRING(this FROM 1 FOR 6) FROM map", SqlColumnType.VARCHAR, "abcde");
        checkValueInternal("SELECT SUBSTRING(this FROM 1 FOR null) FROM map", SqlColumnType.VARCHAR, "abcde");
        checkFailureInternal("SELECT SUBSTRING(this FROM 1 FOR -1) FROM map", SqlErrorCode.DATA_EXCEPTION, "SUBSTRING \"length\" operand cannot be negative");

        // Character column
        put('a');
        checkValueInternal("SELECT SUBSTRING(this FROM 1) FROM map", SqlColumnType.VARCHAR, "a");
        checkValueInternal("SELECT SUBSTRING(this FROM 2) FROM map", SqlColumnType.VARCHAR, "");
        checkValueInternal("SELECT SUBSTRING(this FROM 1 FOR 1) FROM map", SqlColumnType.VARCHAR, "a");

        // Null value
        put(new ExpressionValue.StringVal().field1(null));
        checkValueInternal("SELECT SUBSTRING(field1 FROM 1) FROM map", SqlColumnType.VARCHAR, null);

        // Other columns
        put(true);
        checkValueInternal("SELECT SUBSTRING(this FROM 1) FROM map", SqlColumnType.VARCHAR, "true");

        put((byte) 1);
        checkValueInternal("SELECT SUBSTRING(this FROM 1) FROM map", SqlColumnType.VARCHAR, "1");

        put((short) 2);
        checkValueInternal("SELECT SUBSTRING(this FROM 1) FROM map", SqlColumnType.VARCHAR, "2");

        put(3);
        checkValueInternal("SELECT SUBSTRING(this FROM 1) FROM map", SqlColumnType.VARCHAR, "3");

        put(4L);
        checkValueInternal("SELECT SUBSTRING(this FROM 1) FROM map", SqlColumnType.VARCHAR, "4");

        put(new BigInteger("5"));
        checkValueInternal("SELECT SUBSTRING(this FROM 1) FROM map", SqlColumnType.VARCHAR, "5");

        put(new BigDecimal("6"));
        checkValueInternal("SELECT SUBSTRING(this FROM 1) FROM map", SqlColumnType.VARCHAR, "6");

        put(7f);
        checkValueInternal("SELECT SUBSTRING(this FROM 1) FROM map", SqlColumnType.VARCHAR, "7.0");

        put(8f);
        checkValueInternal("SELECT SUBSTRING(this FROM 1) FROM map", SqlColumnType.VARCHAR, "8.0");

        // Parameter
        put(1);
        checkValueInternal("SELECT SUBSTRING(? FROM 1) FROM map", SqlColumnType.VARCHAR, "abcde", "abcde");
        checkValueInternal("SELECT SUBSTRING(? FROM 1) FROM map", SqlColumnType.VARCHAR, "a", 'a');
        checkValueInternal("SELECT SUBSTRING(? FROM 1) FROM map", SqlColumnType.VARCHAR, null, new Object[] { null });

        checkFailureInternal("SELECT SUBSTRING(? FROM 1) FROM map", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from TINYINT to VARCHAR", (byte) 1);
        checkFailureInternal("SELECT SUBSTRING(? FROM 1) FROM map", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from SMALLINT to VARCHAR", (short) 1);
        checkFailureInternal("SELECT SUBSTRING(? FROM 1) FROM map", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from INTEGER to VARCHAR", 1);
        checkFailureInternal("SELECT SUBSTRING(? FROM 1) FROM map", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from BIGINT to VARCHAR", 1L);
        checkFailureInternal("SELECT SUBSTRING(? FROM 1) FROM map", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DECIMAL to VARCHAR", BigInteger.ONE);
        checkFailureInternal("SELECT SUBSTRING(? FROM 1) FROM map", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DECIMAL to VARCHAR", BigDecimal.ONE);
        checkFailureInternal("SELECT SUBSTRING(? FROM 1) FROM map", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from REAL to VARCHAR", 1f);
        checkFailureInternal("SELECT SUBSTRING(? FROM 1) FROM map", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DOUBLE to VARCHAR", 1d);
        checkFailureInternal("SELECT SUBSTRING(? FROM 1) FROM map", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from OBJECT to VARCHAR", new ExpressionValue.ObjectVal());

        // Literal
        checkValueInternal("SELECT SUBSTRING('abc' FROM 1) FROM map", SqlColumnType.VARCHAR, "abc");
        checkValueInternal("SELECT SUBSTRING(1 FROM 1) FROM map", SqlColumnType.VARCHAR, "1");
    }

    @Test
    public void test_start() {
        // TODO: Literals
    }

    @Test
    public void test_length() {
        // TODO: Literals
    }

    @Test
    public void test_parameters() {
        // TODO: Literals
    }
}
