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

package com.hazelcast.sql.impl.expression.predicate;

import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.expression.ExpressionTestSupport;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Comparator;
import java.util.List;

import static com.hazelcast.sql.SqlColumnType.BIGINT;
import static com.hazelcast.sql.SqlColumnType.DATE;
import static com.hazelcast.sql.SqlColumnType.DOUBLE;
import static com.hazelcast.sql.SqlColumnType.INTEGER;
import static com.hazelcast.sql.SqlColumnType.OBJECT;
import static com.hazelcast.sql.SqlColumnType.TIME;
import static com.hazelcast.sql.SqlColumnType.TIMESTAMP;
import static com.hazelcast.sql.SqlColumnType.VARCHAR;
import static org.junit.Assert.assertEquals;

@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BetweenOperatorIntegrationTest extends ExpressionTestSupport {
    static class Person implements Serializable {
        public final String name;

        Person(String name) {
            this.name = name;
        }
    }

    @Test
    public void basicNumericBetweenPredicateTest() {
        putAll(0, 1, 25, 30);
        checkValues(sqlQuery("BETWEEN 2 AND 2"), INTEGER, new Integer[]{});
        checkValues(sqlQuery("BETWEEN 0 AND 25"), INTEGER, new Integer[]{0, 1, 25});
        checkValues(sqlQuery("NOT BETWEEN 25 AND 40"), INTEGER, new Integer[]{0, 1});
        checkValues(sqlQuery("BETWEEN ? AND ?"), INTEGER, new Integer[]{1, 25}, 1, 25);
        checkValues(sqlQuery("NOT BETWEEN ? AND ?"), INTEGER, new Integer[]{0, 30}, 1, 25);

        checkValues(sqlQuery("BETWEEN SYMMETRIC 25 AND 0"), INTEGER, new Integer[]{0, 1, 25});
        checkValues(sqlQuery("BETWEEN SYMMETRIC 0 AND 25"), INTEGER, new Integer[]{0, 1, 25});
        checkValues(sqlQuery("NOT BETWEEN SYMMETRIC 25 AND 0"), INTEGER, new Integer[]{30});
        checkValues(sqlQuery("NOT BETWEEN SYMMETRIC 0 AND 25"), INTEGER, new Integer[]{30});
        checkValues(sqlQuery("BETWEEN ? AND ?"), INTEGER, new Integer[]{0, 1, 25}, 0, 25);
        checkValues(sqlQuery("NOT BETWEEN ? AND ?"), INTEGER, new Integer[]{0, 1, 25, 30}, 25, 0);
    }

    @Test
    public void betweenPredicateNullcheckTest() {
        putAll(new Person(null));
        checkValues("SELECT name FROM map WHERE name BETWEEN NULL AND NULL", OBJECT, new Object[]{});
        checkValues("SELECT name FROM map WHERE name BETWEEN 2 AND NULL", INTEGER, new Integer[]{});
        checkValues("SELECT name FROM map WHERE name BETWEEN NULL AND 2", INTEGER, new Integer[]{});
        checkValues("SELECT name FROM map WHERE name BETWEEN 'abc' AND NULL", VARCHAR, new String[]{});
        checkValues("SELECT name FROM map WHERE name BETWEEN NULL AND 'bcd'", VARCHAR, new String[]{});

        putAll(0, 1, 25, 30);
        checkValues(sqlQuery("BETWEEN NULL AND 2"), INTEGER, new Integer[]{});
        checkValues(sqlQuery("BETWEEN 2 AND NULL"), INTEGER, new Integer[]{});
        checkValues(sqlQuery("BETWEEN NULL AND 2"), INTEGER, new Integer[]{});
        checkValues(sqlQuery("BETWEEN 'abc' AND NULL"), VARCHAR, new String[]{});
        checkValues(sqlQuery("BETWEEN NULL AND 'bcd'"), VARCHAR, new String[]{});
        checkValues(sqlQuery("BETWEEN SYMMETRIC CAST('2020-01-01' AS DATE) AND NULL"), DATE, new LocalDate[]{});
        checkValues(sqlQuery("BETWEEN SYMMETRIC CAST('20:00:00' AS TIME) AND NULL"), TIME, new LocalTime[]{});
        checkValues(sqlQuery("BETWEEN SYMMETRIC CAST('2000-01-01T08:00:00' AS TIMESTAMP) AND NULL"), TIME, new LocalDateTime[]{});
    }

    @Test
    public void betweenPredicateStringTest() {
        putAll("Argentina", "Bulgaria", "Ukraine");
        checkValues(sqlQuery("BETWEEN 'Argentum' AND 'Uranus'"), VARCHAR, new String[]{"Bulgaria", "Ukraine"});
        checkValues(sqlQuery("NOT BETWEEN 'Argentum' AND 'Uranus'"), VARCHAR, new String[]{"Argentina"});
        checkValues(sqlQuery("BETWEEN SYMMETRIC ? AND ?"), VARCHAR, new String[]{"Bulgaria", "Ukraine"}, "Uranus", "Argentum");
        checkValues(sqlQuery("NOT BETWEEN SYMMETRIC ? AND ?"), VARCHAR, new String[]{}, "Argentina", "Uranus");
        checkValues(sqlQuery("BETWEEN 'AAAAA' AND 'ZZZZZ'"), VARCHAR, new String[]{"Argentina", "Bulgaria", "Ukraine"});
        checkValues(sqlQuery("NOT BETWEEN 'AAA' AND 'ZZZ'"), VARCHAR, new String[]{});
        checkValues(sqlQuery("NOT BETWEEN SYMMETRIC 'ZZZ' AND 'AAA'"), VARCHAR, new String[]{});
    }

    @Test
    public void betweenPredicateDatesTest() {
        LocalDate date1 = LocalDate.of(2000, 1, 1);
        LocalDate date2 = LocalDate.of(2010, 1, 1);
        LocalDate date3 = LocalDate.of(2020, 1, 1);
        putAll(date1, date2, date3);

        checkValues(sqlQuery("BETWEEN CAST('2000-01-01' AS DATE) AND CAST('2020-01-01' AS DATE)"), DATE, new LocalDate[]{date1, date2, date3});
        checkValues(sqlQuery("BETWEEN CAST('2000-01-01' AS DATE) AND CAST('2019-01-01' AS DATE)"), DATE, new LocalDate[]{date1, date2});
        checkValues(sqlQuery("BETWEEN CAST('2000-01-02' AS DATE) AND CAST('2019-01-01' AS DATE)"), DATE, new LocalDate[]{date2});
        checkValues(sqlQuery("NOT BETWEEN CAST('2000-01-01' AS DATE) AND CAST('2020-01-01' AS DATE)"), DATE, new LocalDate[]{});
        checkValues(sqlQuery("BETWEEN SYMMETRIC CAST('2020-01-01' AS DATE) AND CAST('2000-01-01' AS DATE)"), DATE, new LocalDate[]{date1, date2, date3});
        checkValues(sqlQuery("BETWEEN SYMMETRIC CAST('2000-01-01' AS DATE) AND CAST('2020-01-01' AS DATE)"), DATE, new LocalDate[]{date1, date2, date3});
        checkValues(sqlQuery("NOT BETWEEN SYMMETRIC CAST('2020-01-01' AS DATE) AND CAST('2000-01-01' AS DATE)"), DATE, new LocalDate[]{});
    }

    @Test
    public void betweenPredicateTimesTest() {
        LocalTime time1 = LocalTime.of(8, 0, 0);
        LocalTime time2 = LocalTime.of(10, 0, 0);
        LocalTime time3 = LocalTime.of(20, 0, 0);
        putAll(time1, time2, time3);

        checkValues(sqlQuery("BETWEEN CAST('08:00:00' AS TIME) AND CAST('20:00:00' AS TIME)"), TIME, new LocalTime[]{time1, time2, time3});
        checkValues(sqlQuery("BETWEEN CAST('08:00:00' AS TIME) AND CAST('19:00:00' AS TIME)"), TIME, new LocalTime[]{time1, time2});
        checkValues(sqlQuery("BETWEEN CAST('08:00:01' AS TIME) AND CAST('19:00:00' AS TIME)"), TIME, new LocalTime[]{time2});
        checkValues(sqlQuery("NOT BETWEEN CAST('08:00:00' AS TIME) AND CAST('20:00:00' AS TIME)"), TIME, new LocalTime[]{});
        checkValues(sqlQuery("BETWEEN SYMMETRIC CAST('20:00:00' AS TIME) AND CAST('08:00:00' AS TIME)"), TIME, new LocalTime[]{time1, time2, time3});
        checkValues(sqlQuery("BETWEEN SYMMETRIC CAST('08:00:00' AS TIME) AND CAST('20:00:00' AS TIME)"), TIME, new LocalTime[]{time1, time2, time3});
        checkValues(sqlQuery("NOT BETWEEN SYMMETRIC CAST('20:00:00' AS TIME) AND CAST('08:00:00' AS TIME)"), TIME, new LocalTime[]{});
    }

    @Test
    public void betweenPredicateTimestampsTest() {
        LocalDateTime time1 = LocalDateTime.of(2000, 1, 1, 8, 0, 0);
        LocalDateTime time2 = LocalDateTime.of(2010, 1, 1, 10, 0, 0);
        LocalDateTime time3 = LocalDateTime.of(2020, 1, 1, 20, 0, 0);
        putAll(time1, time2, time3);

        checkValues(
            sqlQuery("BETWEEN CAST('2000-01-01T08:00:00' AS TIMESTAMP) AND CAST('2020-01-01T20:00:00' AS TIMESTAMP)"),
            TIMESTAMP,
            new LocalDateTime[]{time1, time2, time3}
        );
        checkValues(
            sqlQuery("BETWEEN CAST('2000-01-01T08:00:00' AS TIMESTAMP) AND CAST('2020-01-01T19:00:00' AS TIMESTAMP)"),
            TIMESTAMP,
            new LocalDateTime[]{time1, time2}
        );
        checkValues(
            sqlQuery("BETWEEN CAST('2000-01-01T08:00:01' AS TIMESTAMP) AND CAST('2020-01-01T19:00:00' AS TIMESTAMP)"),
            TIMESTAMP,
            new LocalDateTime[]{time2})
        ;
        checkValues(
            sqlQuery("NOT BETWEEN CAST('2000-01-01T08:00:00' AS TIMESTAMP) AND CAST('2020-01-01T20:00:00' AS TIMESTAMP)"),
            TIMESTAMP,
            new LocalDateTime[]{}
        );
        checkValues(
            sqlQuery("BETWEEN SYMMETRIC CAST('2020-01-01T20:00:00' AS TIMESTAMP) AND CAST('2000-01-01T08:00:00' AS TIMESTAMP)"),
            TIMESTAMP,
            new LocalDateTime[]{time1, time2, time3}
        );
        checkValues(
            sqlQuery("BETWEEN SYMMETRIC CAST('2000-01-01T08:00:00' AS TIMESTAMP) AND CAST('2020-01-01T20:00:00' AS TIMESTAMP)"),
            TIMESTAMP,
            new LocalDateTime[]{time1, time2, time3}
        );
        checkValues(
            sqlQuery("NOT BETWEEN SYMMETRIC CAST('2020-01-01T20:00:00' AS TIMESTAMP) AND CAST('2000-01-01T08:00:00' AS TIMESTAMP)"),
            TIMESTAMP,
            new LocalDateTime[]{}
        );
    }

    @Test
    public void betweenPredicateImplicitCastsAllowedTest() {
        putAll("1", "2", "3");

        checkValues(sqlQuery("BETWEEN 1 AND 3"), VARCHAR, new String[]{"1", "2", "3"});
        checkValues(sqlQuery("BETWEEN SYMMETRIC 3 AND 1"), VARCHAR, new String[]{"1", "2", "3"});

        putAll("1.5", "2.5", "3.25");
        checkValues(sqlQuery("BETWEEN '1' AND '4'"), VARCHAR, new String[]{"1.5", "2.5", "3.25"});
        checkValues(sqlQuery("BETWEEN SYMMETRIC '4' AND '1'"), VARCHAR, new String[]{"1.5", "2.5", "3.25"});

        putAll(1, 2, 3);
        checkValues(sqlQuery("BETWEEN '1' AND '3'"), INTEGER, new Integer[]{1, 2, 3});
        checkValues(sqlQuery("BETWEEN SYMMETRIC '3' AND '1'"), INTEGER, new Integer[]{1, 2, 3});

        putAll(1.5, 2.5, 3.5);
        checkValues(sqlQuery("BETWEEN '0.99' AND '3.51'"), DOUBLE, new Double[]{1.5, 2.5, 3.5});
        checkValues(sqlQuery("BETWEEN SYMMETRIC '3.51' AND '0.99'"), DOUBLE, new Double[]{1.5, 2.5, 3.5});

        putAll(1L, 10L, 100L);
        checkValues(sqlQuery("BETWEEN '1' AND '300'"), BIGINT, new Long[]{1L, 10L, 100L});
        checkValues(sqlQuery("BETWEEN SYMMETRIC '300' AND '1'"), BIGINT, new Long[]{1L, 10L, 100L});
    }

    @Test
    public void betweenPredicateImplicitCastsBannedTest() {
        LocalDate date = LocalDate.of(2000, 1, 1);
        LocalTime time = LocalTime.of(8, 0, 0);
        putAll(time, 1, "Argentina", "Bulgaria", "Ukraine", 2, date);

        checkFailure0(sqlQuery("BETWEEN 1 AND 2"),
            SqlErrorCode.DATA_EXCEPTION,
            "Cannot parse VARCHAR value to BIGINT"
        );
        checkFailure0(
            sqlQuery("BETWEEN SYMMETRIC CAST('2000-01-01' AS DATE) AND 'Ukraine'"),
            SqlErrorCode.DATA_EXCEPTION,
            "Cannot parse VARCHAR value to DATE"
        );
        checkFailure0(
            sqlQuery("BETWEEN SYMMETRIC 2 AND CAST('2000-01-01' AS DATE)"),
            SqlErrorCode.DATA_EXCEPTION,
            "Cannot parse VARCHAR value to DATE"
        );
        checkFailure0(
            sqlQuery("BETWEEN SYMMETRIC CAST('08:00:00' AS TIME) AND 'Bulgaria'"),
            SqlErrorCode.DATA_EXCEPTION,
            "Cannot parse VARCHAR value to TIME"
        );
        checkFailure0(
            sqlQuery("BETWEEN SYMMETRIC 2 AND CAST('08:00:00' AS TIME)"),
            SqlErrorCode.DATA_EXCEPTION,
            "Cannot parse VARCHAR value to TIME"
        );
    }

    @Test
    @Ignore(value = "Un-ignore after engines merge")
    public void rowNumericBetweenPredicateTest() {
        putAll(0, 1, 5, 10, 15, 25, 30);
        checkValues(rowSqlQuery("BETWEEN ROW(0, 0) AND ROW(3, 10)"), INTEGER, new Integer[][]{
            new Integer[]{0, 0}, new Integer[]{1, 1}, new Integer[]{2, 5}, new Integer[]{3, 10},
        });
    }


    protected void checkValues(
        String sql,
        SqlColumnType expectedType,
        Object[] expectedResults,
        Object ... params
    ) {
        List<SqlRow> rows = execute(member, sql, params);
        assertEquals(expectedResults.length, rows.size());
        if (rows.size() == 0) {
            return;
        }

        if (rows.get(0).getObject(0) instanceof Integer) {
            rows.sort(Comparator.comparingInt(a -> a.getObject(0)));
        } else if (rows.get(0).getObject(0) instanceof Comparable) {
            rows.sort((a, b) -> ((Comparable) a.getObject(0)).compareTo(b.getObject(0)));
        }

        for (int i = 0; i < expectedResults.length; i++) {
            SqlRow row = rows.get(i);
            assertEquals(expectedType, row.getMetadata().getColumn(0).getType());
            assertEquals(expectedResults[i], row.getObject(0));
        }
    }

    private String sqlQuery(String inClause) {
        return "SELECT this FROM map WHERE this " + inClause;
    }

    private String rowSqlQuery(String inClause) {
        return "SELECT * FROM map WHERE this " + inClause;
    }

}
