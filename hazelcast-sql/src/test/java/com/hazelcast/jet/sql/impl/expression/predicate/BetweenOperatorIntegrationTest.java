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

package com.hazelcast.jet.sql.impl.expression.predicate;

import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.sql.impl.expression.ExpressionTestSupport;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionBiValue;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionType;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionTypes;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.sql.SqlColumnType.BIGINT;
import static com.hazelcast.sql.SqlColumnType.DATE;
import static com.hazelcast.sql.SqlColumnType.DOUBLE;
import static com.hazelcast.sql.SqlColumnType.INTEGER;
import static com.hazelcast.sql.SqlColumnType.OBJECT;
import static com.hazelcast.sql.SqlColumnType.TIME;
import static com.hazelcast.sql.SqlColumnType.VARCHAR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Test for BETWEEN operator.
 * <p>
 * Hazelcast SQL engine has 13 types in its type system (also OBJECT and
 * NULL, but they don't count in case of BETWEEN operator). BETWEEN
 * operator has 3 arguments and the test should check 13*13*13 = 2197
 * possible arguments type combination.
 * <p>
 * The BETWEEN operator has two possible modes:<ul>
 *     <li>ASYMMETRIC, which is default mode. SQL engine converts "a BETWEEN b
 *     AND c" to "a <= b AND a >= c"
 *
 *     <li>SYMMETRIC. It it can be used in cases where the user is not sure
 *     that c >= b. SQL engine converts "a SYMMETRIC BETWEEN b AND c" to "(a <=
 *     b AND a >= c) OR (a >= b AND a <= c)"
 * </ul>
 *
 * <p>
 * Types are represented by {@link ExpressionType} and its subclasses.
 * Also, there are lists of values to write to an IMap for each type in
 * {@link ExpressionType} subclasses.
 *
 * <p>
 * Testing algorithm:<ul>
 * <li>The test writes to IMap a collection of elements with the desired types.
 * <li>It executes a query with an equivalent predicate not using BETWEEN
 * (see above)
 * <li>It intercepts the result or the exception
 * <li>Finally, the test executes a query with BETWEEN operator and asserts
 * the result or the exception is the same as for the equivalent query.
 *
 * @see #betweenAsymmetricPredicateTypeCheckTest()
 * @see #betweenSymmetricPredicateTypeCheckTest()
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BetweenOperatorIntegrationTest extends ExpressionTestSupport {

    static final ExpressionType<?>[] TESTED_TYPES = ExpressionTypes.allExcept();

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
    public void betweenPredicateNullTest() {
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
    public void betweenPredicateAllowedImplicitCastsTest() {
        putAll("1", "2", "3");
        checkValues(sqlQuery("BETWEEN 1 AND 3"), VARCHAR, new String[]{"1", "2", "3"});
        checkValues(sqlQuery("BETWEEN SYMMETRIC 3 AND 1"), VARCHAR, new String[]{"1", "2", "3"});

        putAll("1.5", "2.5", "3.25");
        checkValues(sqlQuery("BETWEEN 1.0 AND 4"), VARCHAR, new String[]{"1.5", "2.5", "3.25"});
        checkValues(sqlQuery("BETWEEN SYMMETRIC 4 AND 1.0"), VARCHAR, new String[]{"1.5", "2.5", "3.25"});

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
        for (ExpressionType<?> fieldType : TESTED_TYPES) {
            putAll(fieldType.nonNullValues().toArray());
            for (ExpressionType<?> lowerBoundType : TESTED_TYPES) {
                for (ExpressionType<?> upperBoundType : TESTED_TYPES) {
                    ExpressionBiValue biValue = ExpressionBiValue.createBiValue(
                            lowerBoundType.valueFrom(),
                            upperBoundType.valueTo()
                    );

                    Tuple2<List<SqlRow>, HazelcastSqlException> comparisonEquivalentResult = executePossiblyFailingQuery(
                            // the queries have extra spaces so that the errors are on the same positions
                            "SELECT this FROM map WHERE this >=      ? AND this <= ?  ORDER BY this",
                            fieldType.getFieldConverterType().getTypeFamily().getPublicType(),
                            biValue.field1(),
                            biValue.field2()
                    );

                    try {
                        checkSuccessOrFailure("SELECT this FROM map WHERE this BETWEEN ? AND         ?  ORDER BY this",
                                comparisonEquivalentResult, biValue.field1(), biValue.field2());
                    } catch (Throwable e) {
                        throw new AssertionError("For [" + fieldType + ", " + lowerBoundType + ", " + upperBoundType + "]: " + e, e);
                    }
                }
            }
        }
    }

    @Test
    public void betweenSymmetricPredicateTypeCheckTest() {
        int cycleCounter = 0;
        for (ExpressionType<?> fieldType : TESTED_TYPES) {
            putAll(fieldType.nonNullValues().toArray());
            for (ExpressionType<?> lowerBoundType : TESTED_TYPES) {
                for (ExpressionType<?> upperBoundType : TESTED_TYPES) {
                    ++cycleCounter;
                    ExpressionBiValue biValue = ExpressionBiValue.createBiValue(
                            lowerBoundType.valueFrom(),
                            upperBoundType.valueTo()
                    );

                    Tuple2<List<SqlRow>, HazelcastSqlException> comparisonEquivalentResult = executePossiblyFailingQuery(
                            // the queries have extra spaces so that the errors are on the same positions
                            "SELECT this FROM map WHERE (this >=     ? AND this <= ?) OR (this >= ? AND this <= ?) ORDER BY this",
                            fieldType.getFieldConverterType().getTypeFamily().getPublicType(),
                            biValue.field1(),
                            biValue.field2(),
                            biValue.field2(),
                            biValue.field1()
                    );

                    try {
                        checkSuccessOrFailure("SELECT this FROM map WHERE this BETWEEN SYMMETRIC ? AND         ?  ORDER BY this",
                                comparisonEquivalentResult, biValue.field1(), biValue.field2());
                    } catch (Throwable e) {
                        throw new AssertionError("For [" + fieldType + ", " + lowerBoundType + ", " + upperBoundType + "]: " + e, e);
                    }
                }
            }
        }
        assertEquals(TESTED_TYPES.length * TESTED_TYPES.length * TESTED_TYPES.length, cycleCounter);
    }

    @Test
    @Ignore(value = "Un-ignore after ROW() function implementation")
    public void rowNumericBetweenPredicateTest() {
        putAll(0, 1, 5, 10, 15, 25, 30);
        checkValues("SELECT * FROM map WHERE this BETWEEN ROW(0, 0) AND ROW(3, 10)", INTEGER, new Integer[][]{
                new Integer[]{0, 0}, new Integer[]{1, 1}, new Integer[]{2, 5}, new Integer[]{3, 10},
        });
    }

    protected void checkValues(
            String sql,
            SqlColumnType expectedType,
            Object[] expectedResults,
            Object... params
    ) {
        List<SqlRow> rows = execute(sql, params);
        assertEquals(expectedResults.length, rows.size());
        if (rows.size() == 0) {
            return;
        }

        if (rows.get(0).getObject(0) instanceof Integer) {
            rows.sort(Comparator.comparingInt(a -> a.getObject(0)));
        } else if (rows.get(0).getObject(0) instanceof Comparable) {
            rows.sort((a, b) -> ((Comparable<?>) a.getObject(0)).compareTo(b.getObject(0)));
        }

        for (int i = 0; i < expectedResults.length; i++) {
            SqlRow row = rows.get(i);
            assertEquals(expectedType, row.getMetadata().getColumn(0).getType());
            assertEquals(expectedResults[i], row.getObject(0));
        }
    }

    protected void checkSuccessOrFailure(
            String sql,
            Tuple2<List<SqlRow>, HazelcastSqlException> expectedOutcome,
            Object... params
    ) {
        try {
            List<SqlRow> rows = execute(sql, params);
            assertNull(expectedOutcome.f1());
            assertEquals(expectedOutcome.f0().size(), rows.size());
            List<SqlRow> expectedResultsList = expectedOutcome.f0();

            for (int i = 0; i < rows.size(); i++) {
                SqlColumnType expectedType = expectedResultsList.get(i).getMetadata().getColumn(0).getType();
                SqlColumnType actualType = rows.get(i).getMetadata().getColumn(0).getType();
                assertEquals(expectedType, actualType);

                Object actualObject = rows.get(i).getObject(0);
                Object expectedObject = expectedResultsList.get(i).getObject(0);
                assertEquals(expectedObject, actualObject);
            }
        } catch (HazelcastSqlException e) {
            assertNotNull(expectedOutcome.f1());

            // Expected :At line 1, column [5]5: ...
            // Actual   :At line 1, column [6]5: ...
            // To overcome             this ^ we are comparing substrings like
            // "Parameter at position 1 must be of $1 type, but $2 was found (consider adding an explicit CAST)"
            //
            // Expected :The Jet SQL job failed: Execution on a member failed: com.hazelcast.jet.JetException: Exception in ProcessorTasklet{06bd-fcd0-9e82-0001/Project(IMap[public.map])#1}: com.hazelcast.sql.impl.QueryException: ...
            // Actual   :The Jet SQL job failed: Execution on a member failed: com.hazelcast.jet.JetException: Exception in ProcessorTasklet{06bd-fcd0-9e83-0001/Project(IMap[public.map])#1}: com.hazelcast.sql.impl.QueryException: ...
            // To overcome                                                                                                                           this ^ we are comparing substrings like
            // "Cannot compare two OBJECT values, because left operand has class com.hazelcast.jet.sql.impl.support.expressions.ExpressionType$ObjectHolder type and right operand has class java.lang.String type
            int startIndex = e.getMessage().indexOf("Parameter");
            if (startIndex == -1) {
                startIndex = e.getMessage().indexOf("Cannot compare");
            }
            assertEquals(
                    expectedOutcome.f1().getMessage().substring(startIndex),
                    e.getMessage().substring(startIndex)
            );
        }
    }

    /**
     * Execute a query and return either the result, or the exception it threw.
     */
    protected Tuple2<List<SqlRow>, HazelcastSqlException> executePossiblyFailingQuery(
            String sql,
            SqlColumnType firstColumnExpectedType,
            Object... params
    ) {
        try {
            SqlResult result = instance().getSql().execute(sql, params);
            List<SqlRow> rows = new ArrayList<>();
            for (SqlRow row : result) {
                assertEquals(firstColumnExpectedType, row.getMetadata().getColumn(0).getType());
                rows.add(row);
            }
            return tuple2(rows, null);
        } catch (HazelcastSqlException e) {
            return tuple2(Collections.emptyList(), e);
        }
    }

    private String sqlQuery(String betweenClause) {
        return "SELECT this FROM map WHERE this " + betweenClause;
    }

    static class Person implements Serializable {
        public final String name;

        Person(String name) {
            this.name = name;
        }
    }
}
