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

package com.hazelcast.sql.impl.expression.datetime;

import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.expression.ExpressionTestSupport;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ExtractFunctionIntegrationTest extends ExpressionTestSupport {
    @Parameterized.Parameter(0)
    public String field;

    @Parameterized.Parameter(1)
    public String type;

    @Parameterized.Parameter(2)
    public String input;

    @Parameterized.Parameter(3)
    public double expected;

    @Parameterized.Parameters(name = "{index}: EXTRACT({0} FROM {1}''{2}'') == {3}")
    public static Iterable<Object[]> data() {
        Iterable<TestCase> cases = testCases();
        List<Object[]> data = new ArrayList<>();
        for (TestCase c : cases) {
           for (Map.Entry<String, Double> result: c.results.entrySet()) {
                data.add(new Object[] {
                    result.getKey(),   // Field
                    c.type,            // Type
                    c.input,           // Input
                    result.getValue(), // Expected result
                });
           }
        }
        return data;
    }

    private static Iterable<TestCase> testCases() {
        List<TestCase> testCases = new ArrayList<>();
        testCases.add(create("TIMESTAMP", "0001-04-23",
                results(
                        "MILLENNIUM", 1.0,
                        "CENTURY", 1.0,
                        "DECADE", 0.0,
                        "YEAR", 1.0,
                        "ISOYEAR", 1.0,
                        "QUARTER", 2.0,
                        "MONTH", 4.0,
                        "WEEK", 17.0,
                        "DOW", 1.0,
                        "ISODOW", 1.0,
                        "DAY", 23.0,
                        "DOY", 113.0,
                        "HOUR", 0.0,
                        "SECOND", 0.0,
                        "MINUTE", 0.0,
                        "MILLISECOND", 0.0,
                        "MICROSECOND", 0.0,
                        "EPOCH", -62_125_920_000.0
                )
        ));
        testCases.add(create("TIMESTAMP", "0001-04-23 13:40:55",
                results(
                        "MILLENNIUM", 1.0,
                        "CENTURY", 1.0,
                        "DECADE", 0.0,
                        "YEAR", 1.0,
                        "ISOYEAR", 1.0,
                        "QUARTER", 2.0,
                        "MONTH", 4.0,
                        "WEEK", 17.0,
                        "DOW", 1.0,
                        "ISODOW", 1.0,
                        "DAY", 23.0,
                        "DOY", 113.0,
                        "HOUR", 13.0,
                        "MINUTE", 40.0,
                        "SECOND", 55.0,
                        "MILLISECOND", 55_000.0,
                        "MICROSECOND", 55_000_000.0,
                        "EPOCH", -62_125_870_745.0
                )
        ));
        testCases.add(create("TIMESTAMP", "2006-01-01 00:00:00.0",
                results(
                        "MILLENNIUM", 3.0,
                        "CENTURY", 21.0,
                        "DECADE", 200.0,
                        "YEAR", 2006.0,
                        "ISOYEAR", 2005.0,  // ISOYEAR is different than YEAR
                        "QUARTER", 1.0,
                        "MONTH", 1.0,
                        "WEEK", 52.0,       // It belongs to last week of the previous year
                        "DOW", 0.0,
                        "ISODOW", 7.0,
                        "DAY", 1.0,
                        "HOUR", 0.0,
                        "MINUTE", 0.0,
                        "SECOND", 0.0,
                        "MILLISECOND", 0.0,
                        "MICROSECOND", 0.0,
                        "EPOCH", 1_136_073_600.0
                )
        ));
        testCases.add(create("TIMESTAMP", "2001-02-16 20:38:40.123",
                results(
                        "HOUR", 20.0,
                        "MINUTE", 38.0,
                        "SECOND", 40.0,
                        "MILLISECOND", 40_123.0,
                        "MICROSECOND", 40_123_000.0,
                        "EPOCH", 982_355_920.123
                )
        ));
        testCases.add(create("DATE", "2010-10-04",
                results(
                        "MILLENNIUM", 3.0,
                        "CENTURY", 21.0,
                        "DECADE", 201.0,
                        "YEAR", 2010.0,
                        "ISOYEAR", 2010.0,
                        "QUARTER", 4.0,
                        "MONTH", 10.0,
                        "DOW", 1.0,
                        "ISODOW", 1.0,
                        "WEEK", 40.0,
                        "DAY", 4.0,
                        "HOUR", 0.0,
                        "MINUTE", 0.0,
                        "SECOND", 0.0,
                        "MILLISECOND", 0.0,
                        "MICROSECOND", 0.0,
                        "EPOCH", 1_286_150_400.0
                )));
        testCases.add(create("DATE", "2000-12-31",
                results(
                        "DOY", 366.0,
                        "MILLENNIUM", 2.0,
                        "CENTURY", 20.0,
                        "DECADE", 200.0
                )
        ));
        testCases.add(create("DATE", "2001-12-31",
                results(
                        "DOY", 365.0,
                        "MILLENNIUM", 3.0,
                        "CENTURY", 21.0,
                        "DECADE", 200.0
                )
        ));
        testCases.add(create("DATE", "2004-12-31",
                results(
                        "DOY", 366.0
                )
        ));
        testCases.add(create("DATE", "2100-12-31",
                results(
                        "MILLENNIUM", 3.0,
                        "DOY", 365.0
                )
        ));
        testCases.add(create("DATE", "2021-04-17",
                results(
                        "DOW", 6.0,
                        "ISODOW", 6.0
                )
        ));
        testCases.add(create("DATE", "2021-04-18",
                results(
                        "DOW", 0.0,
                        "ISODOW", 7.0
                )
        ));
        testCases.add(create("DATE", "2021-04-19",
                results(
                        "DOW", 1.0,
                        "ISODOW", 1.0
                )
        ));
        testCases.add(create("DATE", "2005-01-01",
                results(
                        "WEEK", 53.0
                )
        ));
        testCases.add(create("DATE", "2006-01-01",
                results(
                        "WEEK", 52.0
                )
        ));
        testCases.add(create("DATE", "2012-12-31",
                results(
                        "WEEK", 1.0
                )
        ));

        return testCases;
    }

    private static TimeZone defaultTimezone;

    @BeforeClass
    public static void setUpClass() {
        defaultTimezone = TimeZone.getDefault();
    }

    @AfterClass
    public static void tearDownClass() {
        TimeZone.setDefault(defaultTimezone);
    }

    @Test
    public void test() {
        put(1);
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

        check(sql(field, literal(type, input)), expected);

        check(sql(field, "?"), expected, object(type, input));
    }

    private <T> void check(String sql, T expectedResult, Object ...parameters) {
        List<SqlRow> rows = execute(member, sql, parameters);
        SqlRow row = rows.get(0);

        SqlColumnType typeOfReceived = row.getMetadata().getColumn(0).getType();

        assertEquals(SqlColumnType.DOUBLE, typeOfReceived);
        assertEquals(expectedResult, row.getObject(0));
    }

    private String sql(Object field, Object source) {
        return String.format("SELECT EXTRACT(%s FROM %s) FROM map", field, source);
    }

    private static String literal(String type, String input) {
        switch (type) {
            case "DATE":
            case "TIMESTAMP":
            case "TIMESTAMP WITH TIME ZONE":
                return type + "'" + input + "'";
            default:
                fail(type + " not supported for test");
                return "";
        }
    }

    // TODO: Rewrite this function
    private static Object object(String type, String input) {
        switch (type) {
            case "DATE":
                return LocalDate.parse(input);
            case "TIMESTAMP":
                try {
                    return LocalDateTime.parse(input.replace(' ', 'T'));
                } catch (DateTimeException e) {
                }
                LocalDate date = LocalDate.parse(input);
                return date.atTime(0, 0, 0);
            case "TIMESTAMP WITH TIME ZONE":
                try {
                    return OffsetDateTime.parse(input);
                } catch (DateTimeException e) {
                }
                try {
                    LocalDateTime dateTime = LocalDateTime.parse(input.replace(' ', 'T'));
                    return dateTime.atOffset(ZoneOffset.UTC);
                } catch (DateTimeException e) {
                }
                return LocalDate.parse(input).atTime(
                        OffsetTime.of(0, 0, 0, 0, ZoneOffset.UTC)
                );
            default:
                fail(type + " not supported for test");
                return "";
        }
    }

    private static TestCase create(String type, String input, Map<String, Double> results) {
        return new TestCase(type, input, results);
    }

    private static Map<String, Double> results(Object ...args) {
        assertEquals(0, args.length % 2);

        Map<String, Double> expectedResults = new HashMap<>();

        for (int i = 0; i < args.length; i += 2) {
            Object arg0 = args[i];
            Object arg1 = args[i + 1];

            assertInstanceOf(String.class, arg0);
            assertInstanceOf(Double.class, arg1);

            expectedResults.put((String) args[i], (Double) args[i + 1]);
        }
        return expectedResults;
    }

    private static class TestCase {
        private final String type;
        private final String input;
        private final Map<String, Double> results;

        private TestCase(String type, String input, Map<String, Double> results) {
            this.type = type;
            this.input = input;
            this.results = results;
        }
    }
}
