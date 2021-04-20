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

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.expression.ExpressionTestSupport;
import com.hazelcast.sql.support.expressions.ExpressionBiValue;
import com.hazelcast.sql.support.expressions.ExpressionType;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import javafx.util.Pair;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static com.hazelcast.sql.SqlColumnType.BIGINT;
import static com.hazelcast.sql.SqlColumnType.DATE;
import static com.hazelcast.sql.SqlColumnType.DOUBLE;
import static com.hazelcast.sql.SqlColumnType.INTEGER;
import static com.hazelcast.sql.SqlColumnType.OBJECT;
import static com.hazelcast.sql.SqlColumnType.TIME;
import static com.hazelcast.sql.SqlColumnType.TIMESTAMP;
import static com.hazelcast.sql.SqlColumnType.TIMESTAMP_WITH_TIME_ZONE;
import static com.hazelcast.sql.SqlColumnType.VARCHAR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;


/**
 * <p> Test organization for BETWEEN operator.
 * <p> Hazelcast SQL engine has 13 types in their type system (also OBJECT and NULL, but they don't count in case of BETWEEN operator)
 * <p> BETWEEN operator has 3 arguments and test should check 13*13*13 = 2197 possible arguments type combination.
 * <p> Test of BETWEEN operator described below.
 *
 * <p> First of all, BETWEEN operator has two possible mods :
 * <p> - ASYMMETRIC, which is default mode. SQL engine converts "a BETWEEN b AND c" to "a <= b AND a >= c"
 * <p> - SYMMETRIC. It it can be used in cases where the user is not sure if c >= b.
 * SQL engine converts "a SYMMETRIC BETWEEN b AND c" to "(a <= b AND a >= c) OR (a >= b AND a <= c)"
 * <p> Types are expressed by {@link com.hazelcast.sql.support.expressions.ExpressionType} and it's subclasses.
 * Also, There are a lists of prepared items to write to IMap for each type in ExpressionType subclasses.
 *
 * <p> First, test writes to IMap prepared collection of elements with concrete types.
 * <p> Second, test launches the query with equivalent comparisons : <code>SELECT this FROM map WHERE this >= arg1 AND this <= arg2}</code> .
 * <p> If exception throws, test catches it and returns exception.
 * <p> If everything good, return gained result as List of SqlRows.
 * <p> Finally, test launches the query with BETWEEN action : "SELECT this FROM map WHERE this BETWEEN arg1 AND arg2" .
 * here, if exception happens, test compares with exception from first query. Otherwise, if query was correct, compare the results.
 *
 * @see #betweenAsymmetricPredicateTypeCheckTest()
 * @see #betweenSymmetricPredicateTypeCheckTest()
 */
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BetweenOperatorIntegrationTest extends ExpressionTestSupport {
    static class Person implements Serializable {
        public final String name;

        Person(String name) {
            this.name = name;
        }
    }

    static class ClassWithSqlColumnType<T> {
        public ExpressionType<T> expressionType;
        public SqlColumnType sqlType;

        ClassWithSqlColumnType(ExpressionType<T> expressionType, SqlColumnType sqlType) {
            this.expressionType = expressionType;
            this.sqlType = sqlType;
        }
    }

    static final ClassWithSqlColumnType<?>[] CLASS_DESCRIPTORS = new ClassWithSqlColumnType<?>[]{
            new ClassWithSqlColumnType<>(new ExpressionType.StringType(), SqlColumnType.VARCHAR),
            new ClassWithSqlColumnType<>(new ExpressionType.BooleanType(), SqlColumnType.BOOLEAN),
            new ClassWithSqlColumnType<>(new ExpressionType.ByteType(), SqlColumnType.TINYINT),
            new ClassWithSqlColumnType<>(new ExpressionType.ShortType(), SqlColumnType.SMALLINT),
            new ClassWithSqlColumnType<>(new ExpressionType.IntegerType(), SqlColumnType.INTEGER),
            new ClassWithSqlColumnType<>(new ExpressionType.LongType(), SqlColumnType.BIGINT),
            new ClassWithSqlColumnType<>(new ExpressionType.BigDecimalType(), SqlColumnType.DECIMAL),
            new ClassWithSqlColumnType<>(new ExpressionType.FloatType(), SqlColumnType.REAL),
            new ClassWithSqlColumnType<>(new ExpressionType.DoubleType(), SqlColumnType.DOUBLE),
            new ClassWithSqlColumnType<>(new ExpressionType.LocalDateType(), SqlColumnType.DATE),
            new ClassWithSqlColumnType<>(new ExpressionType.LocalTimeType(), SqlColumnType.TIME),
            new ClassWithSqlColumnType<>(new ExpressionType.LocalDateTimeType(), TIMESTAMP),
            new ClassWithSqlColumnType<>(new ExpressionType.OffsetDateTimeType(), TIMESTAMP_WITH_TIME_ZONE),
    };

    @Test
    public void basicBetweenPredicateNumericTest() {
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
    public void betweenAsymmetricPredicateTypeCheckTest() {
        for (ClassWithSqlColumnType<?> classDescriptor : CLASS_DESCRIPTORS) {
            putAll(classDescriptor.expressionType.nonNullValues().toArray());
            for (ClassWithSqlColumnType<?> lowerBoundDesc : CLASS_DESCRIPTORS) {
                for (ClassWithSqlColumnType<?> upperBoundDesc : CLASS_DESCRIPTORS) {
                    ExpressionBiValue biValue = ExpressionBiValue.createBiValue(
                            lowerBoundDesc.expressionType.valueFrom(),
                            upperBoundDesc.expressionType.valueTo()
                    );

                    Pair<List<SqlRow>, HazelcastSqlException> comparisonEquivalentResult = checkComparisonEquivalent(
                            sqlComparisonEquivalentQuery(),
                            classDescriptor.sqlType,
                            biValue.field1(),
                            biValue.field2()
                    );

                    checkValues(sqlBetweenQuery(), comparisonEquivalentResult, biValue.field1(), biValue.field2());
                }

            }
        }
    }

    @Test
    public void betweenSymmetricPredicateTypeCheckTest() {
        int cycleCounter = 0;
        for (ClassWithSqlColumnType<?> classDescriptor : CLASS_DESCRIPTORS) {
            putAll(classDescriptor.expressionType.nonNullValues().toArray());
            for (ClassWithSqlColumnType<?> lowerBoundDesc : CLASS_DESCRIPTORS) {
                for (ClassWithSqlColumnType<?> upperBoundDesc : CLASS_DESCRIPTORS) {
                    ++cycleCounter;
                    ExpressionBiValue biValue = ExpressionBiValue.createBiValue(
                            lowerBoundDesc.expressionType.valueFrom(),
                            upperBoundDesc.expressionType.valueTo()
                    );

                    Pair<List<SqlRow>, HazelcastSqlException> comparisonEquivalentResult = checkComparisonEquivalent(
                            sqlSymmetricComparisonEquivalentQuery(),
                            classDescriptor.sqlType,
                            biValue.field1(),
                            biValue.field2(),
                            biValue.field2(),
                            biValue.field1()
                    );

                    checkValues(sqlSymmetricBetweenQuery(), comparisonEquivalentResult, biValue.field1(), biValue.field2());
                }

            }
        }
        assertEquals(13 * 13 * 13, cycleCounter);
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

    protected void checkValues(
        String sql,
        Pair<List<SqlRow>, HazelcastSqlException> expectedResults,
        Object... params
    ) {
        try {
            List<SqlRow> rows = execute(member, sql, params);
            assertNull(expectedResults.getValue());
            assertEquals(expectedResults.getKey().size(), rows.size());
            List<SqlRow> expectedResultsList = expectedResults.getKey();
            for (int i = 0; i < rows.size(); i++) {
                Object actualObject = rows.get(i).getObject(0);
                Object expectedObject = expectedResultsList.get(i).getObject(0);
                assertEquals(expectedObject, actualObject);

                SqlColumnType expectedType = expectedResultsList.get(i).getMetadata().getColumn(0).getType();
                SqlColumnType actualType = rows.get(i).getMetadata().getColumn(0).getType();
                assertEquals(expectedType, actualType);
            }
        } catch (HazelcastSqlException e) {
             assertNotNull(expectedResults.getValue());
             // Expected : ... Parameter at position 0 must be of BIGINT type, but VARCHAR was found
             // Actual   : ... Parameter at position 0 must be of TINYINT type, but VARCHAR was found
             // We are not hard-casting everything to BIGINT, so, we wouldn't use check below.
             // assertEquals(expectedResults.getValue().getMessage(), e.getMessage());
        }

    }

    protected Pair<List<SqlRow>, HazelcastSqlException> checkComparisonEquivalent(
        String sql,
        SqlColumnType expectedType,
        Object... params
    ) {
        try {
            List<SqlRow> rows = execute(member, sql, params);
            for (SqlRow row : rows) {
                // Here we have tested ComparisonPredicate, so we just check the type correctness.
                assertEquals(expectedType, row.getMetadata().getColumn(0).getType());
            }
            return new Pair<>(rows, null);
        } catch (HazelcastSqlException e) {
            return new Pair<>(Collections.emptyList(), e);
        } finally {
            assertEquals("Impossible situation, not reachable", 0, 0);
        }
    }

    private String sqlQuery(String inClause) {
        return "SELECT this FROM map WHERE this " + inClause;
    }

    private String sqlBetweenQuery() {
        return "SELECT this FROM map WHERE this BETWEEN ? AND         ?";
    }

    private String sqlComparisonEquivalentQuery() {
        return "SELECT this FROM map WHERE this >=      ? AND this <= ?";
    }

    private String sqlSymmetricBetweenQuery() {
        return "SELECT this FROM map WHERE this BETWEEN ? AND         ?";
    }

    private String sqlSymmetricComparisonEquivalentQuery() {
        return "SELECT this FROM map WHERE (this >=     ? AND this <= ?) OR (this <= ? AND this >= ?)";
    }

    private String rowSqlQuery(String inClause) {
        return "SELECT * FROM map WHERE this " + inClause;
    }


}
