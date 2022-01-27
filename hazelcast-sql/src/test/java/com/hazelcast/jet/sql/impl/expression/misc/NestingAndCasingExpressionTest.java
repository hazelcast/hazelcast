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

package com.hazelcast.jet.sql.impl.expression.misc;

import com.hazelcast.jet.sql.impl.expression.ExpressionTestSupport;
import com.hazelcast.jet.sql.impl.validate.HazelcastSqlOperatorTable;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.sql.SqlOperator;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.StringJoiner;

import static com.hazelcast.internal.util.StringUtil.lowerCaseInternal;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.fail;

/**
 * Ensure that a nested function call derives the argument types as expected. This might not be the case if the
 * {@code HazelcastOperandTypeCheckerAware} doesn't work properly and the operand type information is not
 * resolved recursively.
 * <p>
 * Also ensures that function names are case-insensitive.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NestingAndCasingExpressionTest extends ExpressionTestSupport {
    @Before
    public void before0() {
        put(1);
    }

    /**
     * Check if there is a dedicated test for every expression.
     */
    @Test
    public void testAllExpressionsCovered() {
        Set<Field> checkedFields = new HashSet<>();
        Set<String> missingMethodNames = new LinkedHashSet<>();

        for (Field field : HazelcastSqlOperatorTable.class.getDeclaredFields()) {
            if (!Modifier.isStatic(field.getModifiers()) || !Modifier.isPublic(field.getModifiers())) {
                continue;
            }

            if (!SqlOperator.class.isAssignableFrom(field.getType())
                    || field.getName().equals("DESC")
                    || field.getName().equals("UNION")
                    || field.getName().equals("UNION_ALL")
                    || field.getName().equals("VALUES")
                    || field.getName().equals("ROW")
                    || field.getName().equals("COLLECTION_TABLE")
                    || field.getName().equals("MAP_VALUE_CONSTRUCTOR")
                    || field.getName().equals("ARGUMENT_ASSIGNMENT")
                    || field.getName().equals("GENERATE_SERIES")
                    || field.getName().equals("GENERATE_STREAM")
                    || field.getName().equals("CSV_FILE")
                    || field.getName().equals("JSON_FLAT_FILE")
                    || field.getName().equals("AVRO_FILE")
                    || field.getName().equals("PARQUET_FILE")
                    || field.getName().equals("EXISTS")
                    || field.getName().equals("DESCRIPTOR")
                    || field.getName().equals("IMPOSE_ORDER")
                    || field.getName().equals("TUMBLE")
                    || field.getName().equals("HOP")
            ) {
                continue;
            }

            checkedFields.add(field);

            String testMethodName = "test_" + field.getName();

            try {
                NestingAndCasingExpressionTest.class.getMethod(testMethodName);
            } catch (NoSuchMethodException e) {
                missingMethodNames.add(testMethodName);
            }
        }

        assertFalse("No fields to check found!", checkedFields.isEmpty());

        if (!missingMethodNames.isEmpty()) {
            StringJoiner joiner = new StringJoiner("\n");

            missingMethodNames.forEach(joiner::add);

            fail("Tests not implemented: \n" + joiner);
        }
    }

    @Test
    public void test_CAST() {
        check(sql("CAST(? AS INTEGER) || CAST(? AS INTEGER)"), 1, 1);
    }

    @Test
    public void test_AND() {
        check(sql("(? AND ?) || (? AND ?)"), true, true, true, true);
    }

    @Test
    public void test_OR() {
        check(sql("(? OR ?) || (? OR ?)"), true, true, true, true);
    }

    @Test
    public void test_NOT() {
        check(sql("(NOT(?)) || (NOT(?))"), true, true);
    }

    @Test
    public void test_IN() {
        check(sql("(1 IN (1)) || (1 IN (1))"));
    }

    @Test
    public void test_NOT_IN() {
        check(sql("(1 NOT IN (2)) || (1 NOT IN (2))"));
    }

    @Test
    public void test_EQUALS() {
        check(sql("(CAST(1 AS INT) = ?) || (1 = CAST(? AS TINYINT))"), 1, 1);
    }

    @Test
    public void test_NOT_EQUALS() {
        check(sql("(CAST(1 AS INT) != ?) || (1 != CAST(? AS TINYINT))"), 1, 1);
    }

    @Test
    public void test_GREATER_THAN() {
        check(sql("(CAST(1 AS INT) > ?) || (1 > CAST(? AS TINYINT))"), 1, 1);
    }

    @Test
    public void test_GREATER_THAN_OR_EQUAL() {
        check(sql("(CAST(1 AS INT) >= ?) || (1 >= CAST(? AS TINYINT))"), 1, 1);
    }

    @Test
    public void test_LESS_THAN() {
        check(sql("(CAST(1 AS INT) < ?) || (1 < CAST(? AS TINYINT))"), 1, 1);
    }

    @Test
    public void test_LESS_THAN_OR_EQUAL() {
        check(sql("(CAST(1 AS INT) <= ?) || (1 <= CAST(? AS TINYINT))"), 1, 1);
    }

    @Test
    public void test_PLUS() {
        check(sql("(1+?) || (1+?)"), 1, 1);
    }

    @Test
    public void test_MINUS() {
        check(sql("(1-?) || (1-?)"), 1, 1);
    }

    @Test
    public void test_MULTIPLY() {
        check(sql("(1*?) || (1*?)"), 1, 1);
    }

    @Test
    public void test_DIVIDE() {
        check(sql("(1/?) || (1/?)"), 1, 1);
    }

    @Test
    public void test_REMAINDER() {
        check(sql("(1%?) || (1%?)"), 1, 1);
    }

    @Test
    public void test_UNARY_PLUS() {
        check(sql("(+?) || (+?)"), 1, 1);
    }

    @Test
    public void test_UNARY_MINUS() {
        check(sql("(-?) || (-?)"), 1, 1);
    }

    @Test
    public void test_IS_TRUE() {
        check(sql("(? IS TRUE) || (? IS TRUE)"), false, false);
    }

    @Test
    public void test_IS_NOT_TRUE() {
        check(sql("(? IS NOT TRUE) || (? IS NOT TRUE)"), false, false);
    }

    @Test
    public void test_IS_FALSE() {
        check(sql("(? IS FALSE) || (? IS FALSE)"), false, false);
    }

    @Test
    public void test_IS_NOT_FALSE() {
        check(sql("(? IS NOT FALSE) || (? IS NOT FALSE)"), false, false);
    }

    @Test
    public void test_IS_NULL() {
        check(sql("(? IS NULL) || (? IS NULL)"), 1, 1);
    }

    @Test
    public void test_IS_NOT_NULL() {
        check(sql("(? IS NOT NULL) || (? IS NOT NULL)"), 1, 1);
    }

    @Test
    public void test_ABS() {
        check(sql("ABS(?) || ABS(?)"), 1, 1);
    }

    @Test
    public void test_SIGN() {
        check(sql("SIGN(?) || SIGN(?)"), 1, 1);
    }

    @Test
    public void test_RAND() {
        check(sql("RAND(?) || RAND(?)"), 1, 1);
    }

    @Test
    public void test_COS() {
        check(sql("COS(?) || COS(?)"), 1, 1);
    }

    @Test
    public void test_POWER() {
        check(sql("POWER(?, ?) || POWER(?, ?)"), 1, 1, 1, 1);
    }

    @Test
    public void test_SQUARE() {
        check(sql("SQUARE(?) || SQUARE(?)"), 1, 1);
    }

    @Test
    public void test_SQRT() {
        check(sql("SQRT(?) || SQRT(?)"), 4, 4);
    }

    @Test
    public void test_CBRT() {
        check(sql("CBRT(?) || CBRT(?)"), 8, 8);
    }

    @Test
    public void test_SIN() {
        check(sql("SIN(?) || SIN(?)"), 1, 1);
    }

    @Test
    public void test_TAN() {
        check(sql("TAN(?) || TAN(?)"), 1, 1);
    }

    @Test
    public void test_COT() {
        check(sql("COT(?) || COT(?)"), 1, 1);
    }

    @Test
    public void test_ACOS() {
        check(sql("ACOS(?) || ACOS(?)"), 1, 1);
    }

    @Test
    public void test_ASIN() {
        check(sql("ASIN(?) || ASIN(?)"), 1, 1);
    }

    @Test
    public void test_ATAN() {
        check(sql("ATAN(?) || ATAN(?)"), 1, 1);
    }

    @Test
    public void test_ATAN2() {
        check(sql("ATAN2(?, ?) || ATAN2(?, ?)"), 1, 1, 1, 1);
    }

    @Test
    public void test_EXP() {
        check(sql("EXP(?) || EXP(?)"), 1, 1);
    }

    @Test
    public void test_LN() {
        check(sql("LN(?) || LN(?)"), 1, 1);
    }

    @Test
    public void test_LOG10() {
        check(sql("LOG10(?) || LOG10(?)"), 1, 1);
    }

    @Test
    public void test_DEGREES() {
        check(sql("DEGREES(?) || DEGREES(?)"), 1, 1);
    }

    @Test
    public void test_RADIANS() {
        check(sql("RADIANS(?) || RADIANS(?)"), 1, 1);
    }

    @Test
    public void test_FLOOR() {
        check(sql("FLOOR(?) || FLOOR(?)"), 1, 1);
    }

    @Test
    public void test_CEIL() {
        check(sql("CEIL(?) || CEIL(?)"), 1, 1);
    }

    @Test
    public void test_ROUND() {
        check(sql("ROUND(?) || ROUND(?)"), 1, 1);
    }

    @Test
    public void test_TRUNCATE() {
        check(sql("TRUNCATE(?) || TRUNCATE(?)"), 1, 1);
    }

    @Test
    public void test_CONCAT() {
        check(sql("(? || ?) || (? || ?)"), "a", "b", "c", "d");
    }

    @Test
    public void test_CONCAT_WS() {
        check(sql("CONCAT_WS(?, ?, ?) || CONCAT_WS(?, ?, ?)"), SEPARATOR_VAL, "1", "2", SEPARATOR_VAL, "3", "4");
    }

    @Test
    public void test_LIKE() {
        check(sql("(? LIKE ?) || (? LIKE ?)"), "a", "a", "b", "b");
    }

    @Test
    public void test_NOT_LIKE() {
        check(sql("(? NOT LIKE ?) || (? NOT LIKE ?)"), "a", "a", "b", "b");
    }

    @Test
    public void test_SUBSTRING() {
        check(sql("SUBSTRING(? FROM ?) || SUBSTRING(? FROM ?) "), "a", 1, "b", 1);
    }

    @Test
    public void test_TRIM() {
        check(sql("TRIM(LEADING ? FROM ?) || RTRIM(?)"), "1", "1", "2");
    }

    @Test
    public void test_RTRIM() {
        check(sql("RTRIM(?) || RTRIM(?)"), "1", "1");
    }

    @Test
    public void test_LTRIM() {
        check(sql("LTRIM(?) || LTRIM(?)"), "1", "1");
    }

    @Test
    public void test_BTRIM() {
        check(sql("BTRIM(?) || BTRIM(?)"), "1", "1");
    }

    @Test
    public void test_ASCII() {
        check(sql("ASCII(?) || ASCII(?)"), "1", "1");
    }

    @Test
    public void test_INITCAP() {
        check(sql("INITCAP(?) || INITCAP(?)"), "1", "1");
    }

    @Test
    public void test_CHAR_LENGTH() {
        check(sql("CHAR_LENGTH(?) || CHAR_LENGTH(?)"), "1", "1");
    }

    @Test
    public void test_CHARACTER_LENGTH() {
        check(sql("CHARACTER_LENGTH(?) || CHARACTER_LENGTH(?)"), "1", "1");
    }

    @Test
    public void test_LENGTH() {
        check(sql("LENGTH(?) || LENGTH(?)"), "1", "1");
    }

    @Test
    public void test_LOWER() {
        check(sql("LOWER(?) || LOWER(?)"), "1", "2");
    }

    @Test
    public void test_UPPER() {
        check(sql("UPPER(?) || UPPER(?)"), "1", "2");
    }

    @Test
    public void test_REPLACE() {
        check(sql("REPLACE(?, ?, ?) || REPLACE(?, ?, ?) "),
                "xyz", "x", "X", "xyz", "y", "Y");
    }

    @Test
    public void test_POSITION() {
        check(sql("POSITION(? IN ?) || POSITION(? IN ?)"),
                "y", "xyz", "z", "xyz");
    }

    @Test
    public void test_BETWEEN_ASYMMETRIC() {
        check(sqlWithWhere("BETWEEN ? AND ? "), SqlColumnType.INTEGER, 1, 1);
    }

    @Test
    public void test_NOT_BETWEEN_ASYMMETRIC() {
        check(sqlWithWhere("NOT BETWEEN ? AND ? "), SqlColumnType.INTEGER, 2, 2);
    }

    @Test
    public void test_BETWEEN_SYMMETRIC() {
        check(sqlWithWhere("BETWEEN SYMMETRIC ? AND ? "), SqlColumnType.INTEGER, 1, 1);
    }

    @Test
    public void test_NOT_BETWEEN_SYMMETRIC() {
        check(sqlWithWhere("NOT BETWEEN SYMMETRIC ? AND ? "), SqlColumnType.INTEGER, 2, 2);
    }

    @Test
    public void test_CASE() {
        check(sql("CASE WHEN ? THEN ? END || CASE WHEN ? THEN ? END"), true, "foo", true, "bar");
    }

    @Test
    public void test_EXTRACT() {
        check(sql("EXTRACT(MONTH FROM ?) || EXTRACT(MONTH FROM ?)"),
                LocalDateTime.now(), LocalDateTime.now());
    }

    @Test
    public void test_NULLIF() {
        check(sql("NULLIF(1, ?) || NULLIF(1, ?)"), 1, 2);
    }

    @Test
    public void test_COALESCE() {
        check(sql("COALESCE('1', ?) || COALESCE('2', ?)"), "1", "2");
    }

    @Test
    public void test_TO_TIMESTAMP_TZ() {
        check(sql("TO_TIMESTAMP_TZ(?) || TO_TIMESTAMP_TZ(?)"), 1L, 1L);
    }

    @Test
    public void test_TO_EPOCH_MILLIS() {
        check(sql("TO_EPOCH_MILLIS(?) || TO_EPOCH_MILLIS(?)"), OFFSET_DATE_TIME_VAL, OFFSET_DATE_TIME_VAL);
    }

    @Test
    public void test_COUNT() {
        check(sql("COUNT(?) || COUNT(?)"), 1L, 1L);
    }

    @Test
    public void test_SUM() {
        check(sql("SUM(?) || SUM(?)"), 1L, 1L);
    }

    @Test
    public void test_AVG() {
        check(sql("AVG(?) || AVG(?)"), 1L, 1L);
    }

    @Test
    public void test_MIN() {
        check(sql("MIN(?) || MIN(?)"), 1L, 1L);
    }

    @Test
    public void test_MAX() {
        check(sql("MAX(?) || MAX(?)"), 1L, 1L);
    }

    @Test
    public void test_JSON_QUERY() {
        check(sql("JSON_QUERY(CAST(? AS JSON), '$') || JSON_QUERY(CAST(? AS JSON), '$')"), "[1]", "[1]");
    }

    @Test
    public void test_JSON_VALUE() {
        check(sql("JSON_VALUE(CAST(? AS JSON), '$[0]') || JSON_VALUE(CAST(? AS JSON), '$[0]')"), "[1]", "[1]");
    }

    @Test
    public void test_JSON_OBJECT() {
        check(sql("JSON_OBJECT(? : ?) || JSON_OBJECT(KEY ? VALUE ?)"), "k", "v", "k", "v");
    }

    @Test
    public void test_JSON_ARRAY() {
        check(sql("JSON_ARRAY(?) || JSON_ARRAY(?)"), "v", "v");
    }

    private void check(String sql, Object... params) {
        checkValue0(sql, SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, params);
        checkValue0(lowerCaseInternal(sql), SqlColumnType.VARCHAR, SKIP_VALUE_CHECK, params);
    }

    private void check(String sql, SqlColumnType type, Object... params) {
        checkValue0(sql, type, SKIP_VALUE_CHECK, params);
        checkValue0(lowerCaseInternal(sql), type, SKIP_VALUE_CHECK, params);
    }

    private String sql(String expression) {
        return "SELECT " + expression + " FROM map";
    }

    private String sqlWithWhere(String expression) {
        return "SELECT this FROM map WHERE this " + expression;
    }

}
