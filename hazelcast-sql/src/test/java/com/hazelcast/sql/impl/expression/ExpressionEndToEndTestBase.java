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

import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.SqlException;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class ExpressionEndToEndTestBase extends SqlTestSupport {

    private static final Record RECORD = new Record();

    private static SqlService sql;

    private static TestHazelcastInstanceFactory factory;

    @BeforeClass
    public static void beforeClass() {
        factory = new TestHazelcastInstanceFactory(2);
        sql = factory.newHazelcastInstance(smallInstanceConfig()).getSql();
        IMap<Integer, Record> records = factory.newHazelcastInstance(smallInstanceConfig()).getMap("records");
        records.put(0, RECORD);
    }

    @AfterClass
    public static void afterClass() {
        factory.shutdownAll();
    }

    protected static void assertRow(String expression, SqlColumnType expectedType, Object expectedValue, Object... args) {
        boolean done = false;
        for (SqlRow row : sql.query("select " + expression + " from records", args)) {
            if (done) {
                fail("one row expected");
            }
            done = true;

            assertEquals(1, row.getMetadata().getColumnCount());
            assertEquals(expectedType, row.getMetadata().getColumn(0).getType());
            assertEquals(expectedValue, row.getObject(0));
        }
    }

    protected static void assertParsingError(String expression, String message, Object... args) {
        assertError(expression, SqlErrorCode.PARSING, message, args);
    }

    protected static void assertDataError(String expression, String message, Object... args) {
        assertError(expression, SqlErrorCode.DATA_EXCEPTION, message, args);
    }

    protected static void assertError(String expression, int errorCode, String message, Object... args) {
        try {
            //noinspection StatementWithEmptyBody
            for (SqlRow ignore : sql.query("select " + expression + " from records", args)) {
                // do nothing
            }
        } catch (SqlException e) {
            assertEquals(errorCode, e.getCode());
            assertTrue("expected message '" + message + "', got '" + e.getMessage() + "'",
                    e.getMessage().toLowerCase().contains(message));
            return;
        }
        fail("expected error: " + message);
    }

    public static final class Record implements Serializable {

        public boolean booleanTrue = true;

        public byte byte1 = 1;
        public short short1 = 1;
        public int int1 = 1;
        public long long1 = 1;

        public float float1 = 1;
        public double double1 = 1;

        public BigDecimal decimal1 = BigDecimal.valueOf(1);
        public BigInteger bigInteger1 = BigInteger.valueOf(1);

        public String string1 = "1";
        public char char1 = '1';

    }

    public static final class SerializableObject implements Serializable {

    }

}
