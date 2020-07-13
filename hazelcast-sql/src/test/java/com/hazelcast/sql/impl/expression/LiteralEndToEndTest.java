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

import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.SqlException;
import com.hazelcast.sql.SqlRow;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LiteralEndToEndTest extends ExpressionEndToEndTestBase {

    @Test
    public void testValid() {
        check("0", TINYINT, (byte) 0);
        check("-0", TINYINT, (byte) 0);
        check("000", TINYINT, (byte) 0);
        check("1", TINYINT, (byte) 1);
        check("-1", TINYINT, (byte) -1);
        check("-01", TINYINT, (byte) -1);
        check("001", TINYINT, (byte) 1);
        check("100", TINYINT, (byte) 100);
        check(Byte.toString(Byte.MAX_VALUE), TINYINT, Byte.MAX_VALUE);

        check(Short.toString((short) (Byte.MAX_VALUE + 1)), SMALLINT, (short) (Byte.MAX_VALUE + 1));
        check(Short.toString(Short.MAX_VALUE), SMALLINT, Short.MAX_VALUE);

        check(Integer.toString(Short.MAX_VALUE + 1), INT, Short.MAX_VALUE + 1);
        check(Integer.toString(Integer.MAX_VALUE), INT, Integer.MAX_VALUE);

        check(Long.toString(Integer.MAX_VALUE + 1L), BIGINT, Integer.MAX_VALUE + 1L);
        check(Long.toString(Long.MAX_VALUE), BIGINT, Long.MAX_VALUE);

        check("1.0", DOUBLE, 1.0);
        check("1.000", DOUBLE, 1.0);
        check("001.000", DOUBLE, 1.0);
        check("1.1", DOUBLE, 1.1);
        check("1.100", DOUBLE, 1.1);
        check("001.100", DOUBLE, 1.1);
        check("1e1", DOUBLE, 1e1);
        check("-0.0", DOUBLE, 0.0);
        check("-1.0", DOUBLE, -1.0);
        check("-001.100", DOUBLE, -1.1);
        check(".0", DOUBLE, 0.0);
        check(".1", DOUBLE, 0.1);

        check("false", BOOLEAN, false);
        check("true", BOOLEAN, true);
        check("tRuE", BOOLEAN, true);

        check("''", VARCHAR, "");
        check("'foo'", VARCHAR, "foo");

        check("null", NULL, null);
        check("nUlL", NULL, null);
    }

    @Test
    public void testInvalid() {
        check(Long.MAX_VALUE + "0", "out of range");
        check("0..0", "was expecting one of");
        check("'foo", "was expecting one of");
    }

    private static void check(String literal, SqlColumnType expectedType, Object expectedValue) {
        boolean done = false;
        for (SqlRow row : sql.query("select " + literal + " from records")) {
            if (done) {
                fail("one row expected");
            }
            done = true;

            assertEquals(1, row.getMetadata().getColumnCount());
            assertEquals(expectedType, row.getMetadata().getColumn(0).getType());
            assertEquals(expectedValue, row.getObject(0));
        }
    }

    private static void check(String literal, String message) {
        try {
            //noinspection StatementWithEmptyBody
            for (SqlRow ignore : sql.query("select " + literal + " from records")) {
                // do nothing
            }
        } catch (SqlException e) {
            assertEquals(SqlErrorCode.PARSING, e.getCode());
            assertTrue("expected message '" + message + "', got '" + e.getMessage() + "'",
                    e.getMessage().toLowerCase().contains(message));
            return;
        }
        fail("expected error: " + message);
    }

}
