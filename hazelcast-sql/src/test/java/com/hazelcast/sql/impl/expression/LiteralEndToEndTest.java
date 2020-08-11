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

package com.hazelcast.sql.impl.expression;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.sql.SqlColumnType.BIGINT;
import static com.hazelcast.sql.SqlColumnType.BOOLEAN;
import static com.hazelcast.sql.SqlColumnType.DOUBLE;
import static com.hazelcast.sql.SqlColumnType.INT;
import static com.hazelcast.sql.SqlColumnType.NULL;
import static com.hazelcast.sql.SqlColumnType.SMALLINT;
import static com.hazelcast.sql.SqlColumnType.TINYINT;
import static com.hazelcast.sql.SqlColumnType.VARCHAR;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LiteralEndToEndTest extends ExpressionEndToEndTestBase {

    @Test
    public void testValid() {
        assertRow("0", EXPR0, TINYINT, (byte) 0);
        assertRow("-0", EXPR0, TINYINT, (byte) 0);
        assertRow("000", EXPR0, TINYINT, (byte) 0);
        assertRow("1", EXPR0, TINYINT, (byte) 1);
        assertRow("-1", EXPR0, TINYINT, (byte) -1);
        assertRow("-01", EXPR0, TINYINT, (byte) -1);
        assertRow("001", EXPR0, TINYINT, (byte) 1);
        assertRow("100", EXPR0, TINYINT, (byte) 100);
        assertRow(Byte.toString(Byte.MAX_VALUE), EXPR0, TINYINT, Byte.MAX_VALUE);

        assertRow(Short.toString((short) (Byte.MAX_VALUE + 1)), EXPR0, SMALLINT, (short) (Byte.MAX_VALUE + 1));
        assertRow(Short.toString(Short.MAX_VALUE), EXPR0, SMALLINT, Short.MAX_VALUE);

        assertRow(Integer.toString(Short.MAX_VALUE + 1), EXPR0, INT, Short.MAX_VALUE + 1);
        assertRow(Integer.toString(Integer.MAX_VALUE), EXPR0, INT, Integer.MAX_VALUE);

        assertRow(Long.toString(Integer.MAX_VALUE + 1L), EXPR0, BIGINT, Integer.MAX_VALUE + 1L);
        assertRow(Long.toString(Long.MAX_VALUE), EXPR0, BIGINT, Long.MAX_VALUE);

        assertRow("1.0", EXPR0, DOUBLE, 1.0);
        assertRow("1.000", EXPR0, DOUBLE, 1.0);
        assertRow("001.000", EXPR0, DOUBLE, 1.0);
        assertRow("1.1", EXPR0, DOUBLE, 1.1);
        assertRow("1.100", EXPR0, DOUBLE, 1.1);
        assertRow("001.100", EXPR0, DOUBLE, 1.1);
        assertRow("1e1", EXPR0, DOUBLE, 1e1);
        assertRow("-0.0", EXPR0, DOUBLE, 0.0);
        assertRow("-1.0", EXPR0, DOUBLE, -1.0);
        assertRow("-001.100", EXPR0, DOUBLE, -1.1);
        assertRow(".0", EXPR0, DOUBLE, 0.0);
        assertRow(".1", EXPR0, DOUBLE, 0.1);

        assertRow("false", EXPR0, BOOLEAN, false);
        assertRow("true", EXPR0, BOOLEAN, true);
        assertRow("tRuE", EXPR0, BOOLEAN, true);

        assertRow("''", EXPR0, VARCHAR, "");
        assertRow("'foo'", EXPR0, VARCHAR, "foo");

        assertRow("null", EXPR0, NULL, null);
        assertRow("nUlL", EXPR0, NULL, null);
    }

    @Test
    public void testInvalid() {
        assertParsingError(Long.MAX_VALUE + "0", "out of range");
        assertParsingError("0..0", "was expecting one of");
        assertParsingError("'foo", "was expecting one of");
    }

}
