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

package com.hazelcast.jet.sql.impl.expression;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.jet.sql.impl.util.ExpressionBiValue;
import com.hazelcast.jet.sql.impl.util.ExpressionValue;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.core.JetTestSupport.ditchJob;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings({"unchecked", "rawtypes"})
public abstract class ExpressionTestSupport extends SqlTestSupport {
    private static final ILogger SUPPORT_LOGGER = Logger.getLogger(ExpressionTestSupport.class);

    public static final Character CHAR_VAL = 'f';
    public static final String STRING_VAL = "foo";
    public static final String SEPARATOR_VAL = "-";
    public static final boolean BOOLEAN_VAL = true;
    public static final byte BYTE_VAL = (byte) 1;
    public static final short SHORT_VAL = (short) 1;
    public static final int INTEGER_VAL = 1;
    public static final long LONG_VAL = 1L;
    public static final BigInteger BIG_INTEGER_VAL = BigInteger.ONE;
    public static final BigDecimal BIG_DECIMAL_VAL = BigDecimal.ONE;
    public static final float FLOAT_VAL = 1f;
    public static final double DOUBLE_VAL = 1d;
    public static final LocalDate LOCAL_DATE_VAL = LocalDate.parse("2020-01-01");
    public static final LocalTime LOCAL_TIME_VAL = LocalTime.parse("00:00");
    public static final LocalDateTime LOCAL_DATE_TIME_VAL = LocalDateTime.parse("2020-01-01T00:00");
    public static final OffsetDateTime OFFSET_DATE_TIME_VAL = OffsetDateTime.parse("2020-01-01T00:00+00:00");
    public static final ExpressionValue OBJECT_VAL = new ExpressionValue.ObjectVal();

    protected static final Object SKIP_VALUE_CHECK = new Object();
    protected static final String STANDARD_LOCAL_DATE_VAL = "2020-1-1";
    protected static final String STANDARD_LOCAL_TIME_VAL = "0:0:0";
    protected static final String STANDARD_LOCAL_OFFSET_TIME_VAL = "2020-1-1 0:0:0+00:00";
    protected static final String STANDARD_LOCAL_DATE_TIME_VAL = "2020-1-1 0:0:0";

    protected static HazelcastInstance member;

    private static final TestHazelcastFactory factory = new TestHazelcastFactory();
    protected static IMap map;

    @BeforeClass
    public static void beforeClass() {
        member = factory.newHazelcastInstance();
        map = member.getMap("map");
    }

    @After
    public void supportAfter() {
        if (member == null) {
            return;
        }
        // after each test ditch all jobs and objects
        List<Job> jobs = member.getJet().getJobs();
        SUPPORT_LOGGER.info("Ditching " + jobs.size() + " jobs in ExpressionTestSupport.@After: "
                + jobs.stream().map(j -> idToString(j.getId())).collect(joining(", ", "[", "]")));
        for (Job job : jobs) {
            ditchJob(job, member);
        }
        Collection<DistributedObject> objects = member.getDistributedObjects();
        SUPPORT_LOGGER.info("Destroying " + objects.size()
                + " distributed objects in SimpleTestInClusterSupport.@After: "
                + objects.stream().map(o -> o.getServiceName() + "/" + o.getName())
                .collect(Collectors.joining(", ", "[", "]")));
        for (DistributedObject o : objects) {
            o.destroy();
        }
    }

    @AfterClass
    public static void after() {
        factory.terminateAll();
    }

    protected void put(Object value) {
        put(0, value);
    }

    protected void put(int key, Object value) {
        map.clear();
        map.put(key, value);

        clearPlanCache(member);
    }

    protected void putAll(Object... values) {
        if (values == null || values.length == 0) {
            return;
        }
        Map<Integer, Object> entries = new HashMap<>();
        int key = 0;
        for (Object value : values) {
            entries.put(key++, value);
        }
        putAll(entries);
    }

    protected void putAll(Map<Integer, Object> entries) {
        map.clear();
        map.putAll(entries);

        clearPlanCache(member);
    }

    protected void putAndCheckValue(
            Object value,
            String sql,
            SqlColumnType expectedType,
            Object expectedResult,
            Object... params
    ) {
        put(value);

        checkValue0(sql, expectedType, expectedResult, params);
    }

    /**
     * Execute a query, assert that it returns exactly 1 row and 1 column. Assert
     * the type of the result and optionally the result value.
     *
     * @param sql            the input query
     * @param expectedType   type of the returned value
     * @param expectedResult expected result value. If it's {@link #SKIP_VALUE_CHECK},
     *                       don't assert the value
     * @param params         query parameters
     *
     * @return the result value
     */
    protected Object checkValue0(
            String sql,
            SqlColumnType expectedType,
            Object expectedResult,
            Object... params
    ) {
        List<SqlRow> rows = execute(member, sql, params);
        assertEquals(1, rows.size());

        SqlRow row = rows.get(0);
        assertEquals(1, row.getMetadata().getColumnCount());
        assertEquals(expectedType, row.getMetadata().getColumn(0).getType());

        Object value = row.getObject(0);

        if (expectedResult != SKIP_VALUE_CHECK) {
            assertEquals(expectedResult, value);
        }

        return value;
    }

    /**
     * Execute a query, assert that it returns only 1 column and the values match the expectedResults array
     * in any order. Assert the type and count of the results.
     *
     * @param sql            the input query
     * @param expectedType   type of the returned value
     * @param expectedResults expected result value. If it's {@link #SKIP_VALUE_CHECK},
     *                       don't assert the value
     * @param params         query parameters
     *
     * @return the result values
     */
    protected Object[] checkValues0(
            String sql,
            SqlColumnType expectedType,
            Object[] expectedResults,
            Object... params
    ) {
        List<SqlRow> rows = execute(member, sql, params);
        int expectedResultCount = expectedResults.length;
        assertEquals(expectedResultCount, rows.size());

        Object[] values = new Object[expectedResultCount];
        for (int i = 0; i < expectedResultCount; i++) {
            SqlRow row = rows.get(i);

            assertEquals(1, row.getMetadata().getColumnCount());
            assertEquals(expectedType, row.getMetadata().getColumn(0).getType());

            Object value = row.getObject(0);
            values[i] = value;
        }
        assertThat(values).containsExactlyInAnyOrderElementsOf(Arrays.asList(expectedResults));

        return values;
    }

    protected void putAndCheckFailure(
            Object value,
            String sql,
            int expectedErrorCode,
            String expectedErrorMessage,
            Object... params
    ) {
        put(value);

        checkFailure0(sql, expectedErrorCode, expectedErrorMessage, params);
    }

    protected void checkFailure0(
            String sql,
            int expectedErrorCode,
            String expectedErrorMessage,
            Object... params
    ) {
        try {
            execute(member, sql, params);

            fail("Must fail");
        } catch (HazelcastSqlException e) {
            assertTrue(expectedErrorMessage.length() != 0);
            assertNotNull(e.getMessage());
            assertTrue(
                    "\nExpected: " + expectedErrorMessage + "\nActual: " + e.getMessage(),
                    e.getMessage().contains(expectedErrorMessage)
            );

            assertEquals(e.getCode() + ": " + e.getMessage(), expectedErrorCode, e.getCode());
        }
    }

    protected static String signatureErrorFunction(String functionName, SqlColumnType... columnTypes) {
        TestCase.assertNotNull(columnTypes);

        StringJoiner joiner = new StringJoiner(", ");
        Arrays.stream(columnTypes).forEach((columnType) -> joiner.add(columnType.toString()));

        return "Cannot apply '" + functionName + "' function to [" + joiner + "] (consider adding an explicit CAST)";
    }

    protected static String signatureErrorOperator(String operatorName, Object... columnTypes) {
        TestCase.assertNotNull(columnTypes);

        StringJoiner joiner = new StringJoiner(", ");
        Arrays.stream(columnTypes).forEach((columnType) -> joiner.add(columnType == SqlColumnType.NULL ? "UNKNOWN" : columnType.toString()));

        return "Cannot apply '" + operatorName + "' operator to [" + joiner + "] (consider adding an explicit CAST)";
    }

    protected static String parameterError(int position, SqlColumnType expectedType, SqlColumnType actualType) {
        return String.format(
                "Parameter at position %d must be of %s type, but %s was found (consider adding an explicit CAST)",
                position,
                expectedType,
                actualType
        );
    }

    protected static ExpressionValue stringValue1(String first) {
        return new ExpressionValue.StringVal().field1(first);
    }

    protected static ExpressionBiValue stringValue2(String first, String second) {
        return new ExpressionBiValue.StringStringVal().fields(first, second);
    }

    protected static ExpressionValue booleanValue1(Boolean first) {
        return new ExpressionValue.BooleanVal().field1(first);
    }

    protected static ExpressionBiValue booleanValue2(Boolean first, Boolean second) {
        return new ExpressionBiValue.BooleanBooleanVal().fields(first, second);
    }
}
