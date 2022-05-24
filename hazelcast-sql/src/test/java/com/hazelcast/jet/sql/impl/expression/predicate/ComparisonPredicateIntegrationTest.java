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

import com.hazelcast.jet.sql.impl.expression.ExpressionTestSupport;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionBiValue;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionType;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionTypes;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionValue;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeUtils;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@SuppressWarnings("rawtypes")
@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ComparisonPredicateIntegrationTest extends ExpressionTestSupport {

    private static final int RES_EQ = 0;
    private static final int RES_LT = -1;
    private static final int RES_GT = 1;
    private static final Integer RES_NULL = null;
    private static final ZoneId DEFAULT_TIME_ZONE = ZoneId.systemDefault();

    @Parameterized.Parameter
    public Mode mode;

    @Parameterized.Parameters(name = "mode:{0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {Mode.EQ},
                {Mode.NEQ},
                {Mode.LT},
                {Mode.LTE},
                {Mode.GT},
                {Mode.GTE},
        });
    }

    protected static final Object[] NUMERICS_QUICK = new Object[]{
            Byte.MIN_VALUE, Byte.MAX_VALUE,
            Short.MIN_VALUE, Short.MAX_VALUE,
            Integer.MIN_VALUE, Integer.MAX_VALUE,
            Long.MIN_VALUE, Long.MAX_VALUE,
            BigDecimal.ONE.negate(), BigDecimal.ONE,
            Float.MIN_VALUE, Float.MAX_VALUE,
            Double.MIN_VALUE, Double.MAX_VALUE,
    };

    protected static final Object[] NUMERICS_SLOW = new Object[]{
            Byte.MIN_VALUE, (byte) -1, (byte) 0, (byte) 1, Byte.MAX_VALUE,
            Short.MIN_VALUE, (short) -1, (short) 0, (short) 1, Short.MAX_VALUE,
            Integer.MIN_VALUE, -1, 0, 1, Integer.MAX_VALUE,
            Long.MIN_VALUE, -1L, 0L, 1L, Long.MAX_VALUE,
            BigInteger.ONE.negate(), BigInteger.ZERO, BigInteger.ONE,
            BigDecimal.ONE.negate(), BigDecimal.ZERO, BigDecimal.ONE,
            Float.MIN_VALUE, -1f, 0f, 1f, Float.MAX_VALUE,
            Double.MIN_VALUE, -1d, 0d, 1d, Double.MAX_VALUE,
    };

    private static final Literal LITERAL_BOOLEAN = new Literal("true", SqlColumnType.BOOLEAN);
    private static final Literal LITERAL_VARCHAR = new Literal("'true'", SqlColumnType.VARCHAR);
    private static final Literal LITERAL_TINYINT = new Literal("1", SqlColumnType.TINYINT);
    private static final Literal LITERAL_DECIMAL = new Literal("1.1", SqlColumnType.DECIMAL);
    private static final Literal LITERAL_DOUBLE = new Literal("1.1E1", SqlColumnType.DOUBLE);

    @Test(timeout = 600_000)
    public void testString() {
        // Column/column
        putCheckCommute(stringValue2("a", "a"), "field1", "field2", RES_EQ);
        putCheckCommute(stringValue2("a", "b"), "field1", "field2", RES_LT);
        putCheckCommute(stringValue2("a", null), "field1", "field2", RES_NULL);
        putCheckCommute(stringValue2(null, null), "field1", "field2", RES_NULL);
        checkUnsupportedColumnColumn(ExpressionTypes.STRING, ExpressionTypes.allExcept(ExpressionTypes.STRING, ExpressionTypes.CHARACTER));

        // Column/literal
        putCheckCommute(stringValue1("a"), "field1", "'a'", RES_EQ);
        putCheckCommute(stringValue1("a"), "field1", "'b'", RES_LT);
        putCheckCommute(stringValue1("a"), "field1", "null", RES_NULL);
        putCheckCommute(stringValue1(null), "field1", "'a'", RES_NULL);
        putCheckCommute(stringValue1(null), "field1", "null", RES_NULL);
        checkUnsupportedColumnLiteral('a', SqlColumnType.VARCHAR, LITERAL_BOOLEAN, LITERAL_TINYINT, LITERAL_DECIMAL, LITERAL_DOUBLE);

        // Column/parameter
        putCheckCommute(stringValue1("a"), "field1", "?", RES_EQ, "a");
        putCheckCommute(stringValue1("a"), "field1", "?", RES_LT, "b");
        putCheckCommute(stringValue1("a"), "field1", "?", RES_NULL, (String) null);
        putCheckCommute(stringValue1(null), "field1", "?", RES_NULL, "a");
        putCheckCommute(stringValue1(null), "field1", "?", RES_NULL, (String) null);
        checkUnsupportedColumnParameter("a", SqlColumnType.VARCHAR, 0, ExpressionTypes.allExcept(ExpressionTypes.STRING, ExpressionTypes.CHARACTER));

        // Literal/literal
        checkCommute("'a'", "'a'", RES_EQ);
        checkCommute("'a'", "'b'", RES_LT);
        checkCommute("'a'", "null", RES_NULL);
        checkCommute("null", "null", RES_NULL);
        checkUnsupportedLiteralLiteral("'a'", SqlColumnType.VARCHAR, LITERAL_BOOLEAN, LITERAL_TINYINT, LITERAL_DECIMAL, LITERAL_DOUBLE);

        // Literal/parameter
        checkCommute("'a'", "?", RES_EQ, "a");
        checkCommute("'a'", "?", RES_LT, "b");
        checkCommute("'a'", "?", RES_NULL, (String) null);
        checkCommute("'a'", "?", RES_NULL, (String) null);
        checkFailure("null", "?", SqlErrorCode.PARSING, signatureErrorOperator(mode.token(), SqlTypeName.NULL, SqlTypeName.UNKNOWN), true);
    }

    @Test(timeout = 600_000)
    public void testBoolean() {
        // Column/column
        putCheckCommute(booleanValue2(true, true), "field1", "field2", RES_EQ);
        putCheckCommute(booleanValue2(true, false), "field1", "field2", RES_GT);
        putCheckCommute(booleanValue2(true, null), "field1", "field2", RES_NULL);
        putCheckCommute(booleanValue2(false, false), "field1", "field2", RES_EQ);
        putCheckCommute(booleanValue2(false, null), "field1", "field2", RES_NULL);
        putCheckCommute(booleanValue2(null, null), "field1", "field2", RES_NULL);
        checkUnsupportedColumnColumn(ExpressionTypes.BOOLEAN, ExpressionTypes.allExcept(ExpressionTypes.BOOLEAN));

        // Column/literal
        putCheckCommute(booleanValue1(true), "field1", "true", RES_EQ);
        putCheckCommute(booleanValue1(true), "field1", "false", RES_GT);
        putCheckCommute(booleanValue1(true), "field1", "null", RES_NULL);
        putCheckCommute(booleanValue1(false), "field1", "true", RES_LT);
        putCheckCommute(booleanValue1(false), "field1", "false", RES_EQ);
        putCheckCommute(booleanValue1(false), "field1", "null", RES_NULL);
        putCheckCommute(booleanValue1(null), "field1", "true", RES_NULL);
        putCheckCommute(booleanValue1(null), "field1", "false", RES_NULL);
        putCheckCommute(booleanValue1(null), "field1", "null", RES_NULL);
        checkUnsupportedColumnLiteral(true, SqlColumnType.BOOLEAN, LITERAL_VARCHAR, LITERAL_TINYINT, LITERAL_DECIMAL, LITERAL_DOUBLE);

        // Column/parameter
        putCheckCommute(booleanValue1(true), "field1", "?", RES_EQ, true);
        putCheckCommute(booleanValue1(true), "field1", "?", RES_GT, false);
        putCheckCommute(booleanValue1(true), "field1", "?", RES_NULL, (Boolean) null);
        putCheckCommute(booleanValue1(false), "field1", "?", RES_LT, true);
        putCheckCommute(booleanValue1(false), "field1", "?", RES_EQ, false);
        putCheckCommute(booleanValue1(false), "field1", "?", RES_NULL, (Boolean) null);
        putCheckCommute(booleanValue1(null), "field1", "?", RES_NULL, true);
        putCheckCommute(booleanValue1(null), "field1", "?", RES_NULL, false);
        putCheckCommute(booleanValue1(null), "field1", "?", RES_NULL, (Boolean) null);
        checkUnsupportedColumnParameter(true, SqlColumnType.BOOLEAN, 0, ExpressionTypes.allExcept(ExpressionTypes.BOOLEAN));

        // Literal/literal
        checkCommute("true", "true", RES_EQ);
        checkCommute("true", "false", RES_GT);
        checkCommute("true", "null", RES_NULL);
        checkCommute("false", "false", RES_EQ);
        checkCommute("false", "null", RES_NULL);
        checkCommute("null", "null", RES_NULL);
        checkUnsupportedLiteralLiteral("true", SqlColumnType.BOOLEAN, LITERAL_VARCHAR, LITERAL_TINYINT, LITERAL_DECIMAL, LITERAL_DOUBLE);

        // Literal/parameter
        checkCommute("true", "?", RES_EQ, true);
        checkCommute("true", "?", RES_GT, false);
        checkCommute("true", "?", RES_NULL, (Boolean) null);
        checkCommute("false", "?", RES_LT, true);
        checkCommute("false", "?", RES_EQ, false);
        checkCommute("false", "?", RES_NULL, (Boolean) null);
        checkFailure("null", "?", SqlErrorCode.PARSING, signatureErrorOperator(mode.token(), SqlTypeName.NULL, SqlTypeName.UNKNOWN), true);
    }

    @Test(timeout = 600_000)
    public void testNumeric() {
        Object[] values = getNumericValues();

        for (int i = 0; i < values.length; i++) {
            for (int j = i; j < values.length; j++) {
                checkNumeric(values[i], values[j]);
            }
        }

        for (ExpressionType type : ExpressionTypes.numeric()) {
            // Column/column
            checkUnsupportedColumnColumn(type, ExpressionTypes.allExcept(ExpressionTypes.numeric()));

            // Column/literal
            SqlColumnType columnType = type.getFieldConverterType().getTypeFamily().getPublicType();
            checkUnsupportedColumnLiteral(type.valueFrom(), columnType, LITERAL_VARCHAR, LITERAL_BOOLEAN);

            // Column/parameter
            if (type.getFieldConverterType().getTypeFamily().getPrecedence() >= QueryDataType.BIGINT.getTypeFamily().getPrecedence()) {
                checkUnsupportedColumnParameter(type.valueFrom(), columnType, 0, ExpressionTypes.allExcept(ExpressionTypes.numeric()));
            }
        }
    }

    @Test
    public void testParameterParameter() {
        put(1);
        checkFailure("?", "?", SqlErrorCode.PARSING, signatureErrorOperator(mode.token(), SqlTypeName.UNKNOWN, SqlTypeName.UNKNOWN), true, true);
    }

    @Test
    public void testUnsupported() {
        // Column/column
        checkUnsupportedColumnColumn(
                ExpressionTypes.LOCAL_DATE, ExpressionTypes.allExcept(
                        ExpressionTypes.LOCAL_DATE,
                        ExpressionTypes.LOCAL_DATE_TIME,
                        ExpressionTypes.OFFSET_DATE_TIME));
        checkUnsupportedColumnColumn(
                ExpressionTypes.LOCAL_TIME, ExpressionTypes.allExcept(
                        ExpressionTypes.LOCAL_TIME,
                        ExpressionTypes.LOCAL_DATE_TIME,
                        ExpressionTypes.OFFSET_DATE_TIME));
        checkUnsupportedColumnColumn(
                ExpressionTypes.LOCAL_DATE_TIME, ExpressionTypes.allExcept(
                        ExpressionTypes.LOCAL_DATE,
                        ExpressionTypes.LOCAL_TIME,
                        ExpressionTypes.LOCAL_DATE_TIME,
                        ExpressionTypes.OFFSET_DATE_TIME));
        checkUnsupportedColumnColumn(
                ExpressionTypes.OFFSET_DATE_TIME, ExpressionTypes.allExcept(
                        ExpressionTypes.LOCAL_DATE,
                        ExpressionTypes.LOCAL_TIME,
                        ExpressionTypes.LOCAL_DATE_TIME,
                        ExpressionTypes.OFFSET_DATE_TIME));
        checkUnsupportedColumnColumn(ExpressionTypes.OBJECT, ExpressionTypes.allExcept(ExpressionTypes.OBJECT));
    }

    @Test
    public void testComparable_to_Comparable() {
        putBiValue(new ComparableImpl(1), new ComparableImpl(1), ExpressionTypes.OBJECT, ExpressionTypes.OBJECT);
        check("field1", "field2", new ComparableImpl(1).compareTo(new ComparableImpl(1)));

        putBiValue(new ComparableImpl(1), new ComparableImpl(2), ExpressionTypes.OBJECT, ExpressionTypes.OBJECT);
        check("field1", "field2", new ComparableImpl(1).compareTo(new ComparableImpl(2)));

        putBiValue(new ComparableImpl(2), new ComparableImpl(1), ExpressionTypes.OBJECT, ExpressionTypes.OBJECT);
        check("field1", "field2", new ComparableImpl(2).compareTo(new ComparableImpl(1)));
    }

    @Test
    public void testComparable_and_NonComparable() {
        putBiValue(new ComparableImpl(1), new NonComparable(), ExpressionTypes.OBJECT, ExpressionTypes.OBJECT);
        checkFailure(
                "field1", "field2", SqlErrorCode.GENERIC,
                "Cannot compare two OBJECT values, because "
                        + "left operand has " + ComparableImpl.class + " type and "
                        + "right operand has " + NonComparable.class + " type");

        putBiValue(new NonComparable(), new ComparableImpl(1), ExpressionTypes.OBJECT, ExpressionTypes.OBJECT);
        checkFailure(
                "field1", "field2", SqlErrorCode.GENERIC,
                "Cannot compare two OBJECT values, because "
                        + "left operand has " + NonComparable.class + " type and "
                        + "right operand has " + ComparableImpl.class + " type");
    }

    @Test
    public void testDifferentClassThatImplementsComparableInterface() {
        putBiValue(new ComparableImpl(1), new ComparableImpl2(1), ExpressionTypes.OBJECT, ExpressionTypes.OBJECT);

        checkFailure(
                "field1", "field2", SqlErrorCode.GENERIC,
                "Cannot compare two OBJECT values, because "
                        + "left operand has " + ComparableImpl.class + " type and "
                        + "right operand has " + ComparableImpl2.class + " type");
    }

    @Test
    public void testNonComparableObjects() {
        putBiValue(new NonComparable(), new NonComparable(), ExpressionTypes.OBJECT, ExpressionTypes.OBJECT);

        checkFailure(
                "field1", "field2", SqlErrorCode.GENERIC,
                "Cannot compare OBJECT value because " + NonComparable.class + " doesn't implement Comparable interface");
    }

    @Test
    public void testCompare_LocalDate_with_LocalDate() {
        putBiValue(
                LocalDate.of(2020, 12, 30),
                LocalDate.of(2020, 12, 30),
                ExpressionTypes.LOCAL_DATE, ExpressionTypes.LOCAL_DATE
        );

        check("field1", "field2", LocalDate.of(2020, 12, 30).compareTo(LocalDate.of(2020, 12, 30)));
    }

    @Test
    public void testCompare_LocalDate_with_String() {
        put(ExpressionValue.create(ExpressionValue.createClass(ExpressionTypes.LOCAL_DATE), LocalDate.of(2020, 12, 30)));

        check("field1", "'2020-12-30'", LocalDate.of(2020, 12, 30).compareTo(LocalDate.of(2020, 12, 30)));
        check("'2020-12-30'", "field1", LocalDate.of(2020, 12, 30).compareTo(LocalDate.of(2020, 12, 30)));
    }

    @Test
    public void testCompare_LocalTime_with_LocalTime() {
        putBiValue(
                LocalTime.of(14, 2, 0),
                LocalTime.of(14, 2, 0),
                ExpressionTypes.LOCAL_TIME, ExpressionTypes.LOCAL_TIME
        );

        check("field1", "field2", LocalTime.of(14, 2, 0).compareTo(LocalTime.of(14, 2, 0)));
    }

    @Test
    public void testCompare_LocalTime_with_String() {
        put(ExpressionValue.create(ExpressionValue.createClass(ExpressionTypes.LOCAL_TIME), LocalTime.of(14, 2, 0)));

        check("field1", "'14:02:00'", LocalTime.of(14, 2, 0).compareTo(LocalTime.of(14, 2, 0)));
        check("'14:02:00'", "field1", LocalTime.of(14, 2, 0).compareTo(LocalTime.of(14, 2, 0)));
    }

    @Test
    public void testCompare_LocalDateTime_with_LocalDateTime() {
        putBiValue(
                LocalDateTime.of(2020, 12, 30, 14, 2, 0),
                LocalDateTime.of(2020, 12, 30, 14, 2, 0),
                ExpressionTypes.LOCAL_DATE_TIME, ExpressionTypes.LOCAL_DATE_TIME
        );

        check("field1", "field2", LocalDateTime.of(2020, 12, 30, 14, 2, 0).compareTo(LocalDateTime.of(2020, 12, 30, 14, 2, 0)));
    }

    @Test
    public void testCompare_LocalDateTime_with_String() {
        put(ExpressionValue.create(ExpressionValue.createClass(ExpressionTypes.LOCAL_DATE_TIME), LocalDateTime.of(2020, 12, 30, 14, 2, 0)));

        check("field1", "'2020-12-30T14:02'", LocalDateTime.of(2020, 12, 30, 14, 2, 0).compareTo(LocalDateTime.of(2020, 12, 30, 14, 2, 0)));
        check("'2020-12-30T14:02'", "field1", LocalDateTime.of(2020, 12, 30, 14, 2, 0).compareTo(LocalDateTime.of(2020, 12, 30, 14, 2, 0)));
    }


    @Test
    public void testCompare_LocalDateTime_with_LocalDate() {
        putBiValue(
                LocalDateTime.of(2020, 12, 30, 14, 2, 0),
                LocalDate.of(2020, 12, 30),
                ExpressionTypes.LOCAL_DATE_TIME, ExpressionTypes.LOCAL_DATE
        );

        check("field1", "field2", LocalDateTime.of(2020, 12, 30, 14, 2, 0).compareTo(LocalDate.of(2020, 12, 30).atStartOfDay()));

        putBiValue(
                LocalDate.of(2020, 12, 30),
                LocalDateTime.of(2020, 12, 30, 14, 2, 0),
                ExpressionTypes.LOCAL_DATE, ExpressionTypes.LOCAL_DATE_TIME
        );

        check("field1", "field2", LocalDate.of(2020, 12, 30).atStartOfDay().compareTo(LocalDateTime.of(2020, 12, 30, 14, 2, 0)));
    }

    @Test
    public void testCompare_LocalDateTime_with_LocalTime() {
        putBiValue(
                LocalDateTime.of(2020, 12, 30, 14, 2, 0),
                LocalTime.of(14, 2, 0),
                ExpressionTypes.LOCAL_DATE_TIME, ExpressionTypes.LOCAL_TIME
        );

        check("cast (field1 as TIME)", "field2", LocalDateTime.of(2020, 12, 30, 14, 2, 0).toLocalTime().compareTo(LocalTime.of(14, 2, 0)));
        check("field1", "field2", LocalDateTime.of(2020, 12, 30, 14, 2, 0).compareTo(LocalDateTime.of(LocalDate.now(), LocalTime.of(14, 2, 0))));

        putBiValue(
                LocalTime.of(14, 2, 0),
                LocalDateTime.of(2020, 12, 30, 14, 2, 0),
                ExpressionTypes.LOCAL_TIME, ExpressionTypes.LOCAL_DATE_TIME
        );

        check("field1", "cast (field2 as TIME)", LocalTime.of(14, 2, 0).compareTo(LocalDateTime.of(2020, 12, 30, 14, 2, 0).toLocalTime()));
        check("field1", "field2", LocalDateTime.of(LocalDate.now(), LocalTime.of(14, 2, 0)).compareTo(LocalDateTime.of(2020, 12, 30, 14, 2, 0)));
    }

    @Test
    public void testCompare_LocalDateTimeWithTZ_with_LocalDateTimeWithTZ() {
        putBiValue(
                OffsetDateTime.of(LocalDateTime.of(2020, 12, 30, 14, 2, 0), ZoneOffset.UTC),
                OffsetDateTime.of(LocalDateTime.of(2020, 12, 30, 14, 2, 0), ZoneOffset.UTC),
                ExpressionTypes.OFFSET_DATE_TIME, ExpressionTypes.OFFSET_DATE_TIME
        );

        check("field1", "field2",
                OffsetDateTime.of(LocalDateTime.of(2020, 12, 30, 14, 2, 0), ZoneOffset.UTC)
                        .compareTo(OffsetDateTime.of(LocalDateTime.of(2020, 12, 30, 14, 2, 0), ZoneOffset.UTC)));
    }

    @Test
    public void testCompare_LocalDateTimeWithTZ_with_String() {
        put(ExpressionValue.create(ExpressionValue.createClass(
                ExpressionTypes.OFFSET_DATE_TIME), OffsetDateTime.of(LocalDateTime.of(2020, 12, 30, 14, 2, 0), ZoneOffset.UTC)));

        check("field1", "'2020-12-30T14:02Z'",
                OffsetDateTime.of(LocalDateTime.of(2020, 12, 30, 14, 2, 0), ZoneOffset.UTC)
                        .compareTo(OffsetDateTime.of(LocalDateTime.of(2020, 12, 30, 14, 2, 0), ZoneOffset.UTC)));
        check("'2020-12-30T14:02Z'", "field1",
                OffsetDateTime.of(LocalDateTime.of(2020, 12, 30, 14, 2, 0), ZoneOffset.UTC)
                        .compareTo(OffsetDateTime.of(LocalDateTime.of(2020, 12, 30, 14, 2, 0), ZoneOffset.UTC)));
    }

    @Test
    public void testCompare_LocalDateTimeWithTZ_with_LocalDateTime() {
        putBiValue(
                OffsetDateTime.of(LocalDateTime.of(2020, 12, 30, 14, 2, 0), ZoneOffset.UTC),
                LocalDateTime.of(2020, 12, 30, 14, 2, 0), ExpressionTypes.OFFSET_DATE_TIME, ExpressionTypes.LOCAL_DATE_TIME
        );

        check("field1", "field2",
                OffsetDateTime.of(LocalDateTime.of(2020, 12, 30, 14, 2, 0), ZoneOffset.UTC)
                        .compareTo(ZonedDateTime.of(LocalDateTime.of(2020, 12, 30, 14, 2, 0), DEFAULT_TIME_ZONE).toOffsetDateTime()));

        putBiValue(
                LocalDateTime.of(2020, 12, 30, 14, 2, 0),
                OffsetDateTime.of(LocalDateTime.of(2020, 12, 30, 14, 2, 0), ZoneOffset.UTC),
                ExpressionTypes.LOCAL_DATE_TIME, ExpressionTypes.OFFSET_DATE_TIME
        );

        check("field1", "field2",
                ZonedDateTime.of(LocalDateTime.of(2020, 12, 30, 14, 2, 0), DEFAULT_TIME_ZONE).toOffsetDateTime()
                        .compareTo(OffsetDateTime.of(LocalDateTime.of(2020, 12, 30, 14, 2, 0), ZoneOffset.UTC)));
    }

    @Test
    public void testCompare_LocalDateTimeWithTZ_with_LocalDate() {
        putBiValue(
                OffsetDateTime.of(LocalDateTime.of(2020, 12, 30, 14, 2, 0), ZoneOffset.UTC),
                LocalDate.of(2020, 12, 30), ExpressionTypes.OFFSET_DATE_TIME, ExpressionTypes.LOCAL_DATE
        );

        check("field1", "field2",
                OffsetDateTime.of(LocalDateTime.of(2020, 12, 30, 14, 2, 0), ZoneOffset.UTC)
                        .compareTo(ZonedDateTime.of(LocalDate.of(2020, 12, 30).atStartOfDay(), DEFAULT_TIME_ZONE).toOffsetDateTime()));

        putBiValue(
                LocalDate.of(2020, 12, 30),
                OffsetDateTime.of(LocalDateTime.of(2020, 12, 30, 14, 2, 0), ZoneOffset.UTC),
                ExpressionTypes.LOCAL_DATE, ExpressionTypes.OFFSET_DATE_TIME
        );

        check("field1", "field2",
                ZonedDateTime.of(LocalDate.of(2020, 12, 30).atStartOfDay(), DEFAULT_TIME_ZONE).toOffsetDateTime()
                        .compareTo(OffsetDateTime.of(LocalDateTime.of(2020, 12, 30, 14, 2, 0), ZoneOffset.UTC)));
    }

    @Test
    public void testCompare_LocalDateTimeWithTZ_with_LocalTime() {
        putBiValue(
                OffsetDateTime.of(LocalDateTime.of(2020, 12, 30, 14, 2, 0), ZoneOffset.UTC),
                LocalTime.of(14, 2, 0), ExpressionTypes.OFFSET_DATE_TIME, ExpressionTypes.LOCAL_TIME
        );

        check("cast (field1 as TIME)", "field2",
                OffsetDateTime.of(LocalDateTime.of(2020, 12, 30, 14, 2, 0), ZoneOffset.UTC).toLocalTime()
                        .compareTo(LocalTime.of(14, 2, 0)));
        check("field1", "field2",
                OffsetDateTime.of(LocalDateTime.of(2020, 12, 30, 14, 2, 0), ZoneOffset.UTC)
                        .compareTo(OffsetDateTime.of(LocalDateTime.of(LocalDate.now(), LocalTime.of(14, 2, 0)), ZoneOffset.UTC)));

        putBiValue(
                LocalTime.of(14, 2, 0),
                OffsetDateTime.of(LocalDateTime.of(2020, 12, 30, 14, 2, 0), ZoneOffset.UTC),
                ExpressionTypes.LOCAL_TIME, ExpressionTypes.OFFSET_DATE_TIME
        );

        check("field1", "cast (field2 as TIME)",
                LocalTime.of(14, 2, 0).compareTo(
                        OffsetDateTime.of(LocalDateTime.of(2020, 12, 30, 14, 2, 0), ZoneOffset.UTC).toLocalTime()));
        check("field1", "field2",
                OffsetDateTime.of(LocalDateTime.of(LocalDate.now(), LocalTime.of(14, 2, 0)), ZoneOffset.UTC)
                        .compareTo(OffsetDateTime.of(LocalDateTime.of(2020, 12, 30, 14, 2, 0), ZoneOffset.UTC)));
    }

    private void putBiValue(Object field1, Object field2, ExpressionType<?> type1, ExpressionType<?> type2) {
        ExpressionBiValue value = ExpressionBiValue.createBiValue(
                ExpressionBiValue.createBiClass(type1, type2),
                field1,
                field2
        );

        put(value);
    }

    protected Object[] getNumericValues() {
        return NUMERICS_QUICK;
    }

    private void checkNumeric(Object value1, Object value2) {
        int res = compareNumeric(value1, value2);

        if (res == RES_EQ && (value1 instanceof Float || value1 instanceof Double || value2 instanceof Float || value2 instanceof Double)) {
            return;
        }

        ExpressionType<?> valueType1 = ExpressionTypes.resolve(value1);
        ExpressionType<?> valueType2 = ExpressionTypes.resolve(value2);

        Class<? extends ExpressionValue> class1 = ExpressionValue.createClass(valueType1);
        Class<? extends ExpressionValue> class2 = ExpressionValue.createClass(valueType2);
        Class<? extends ExpressionBiValue> biClass = ExpressionBiValue.createBiClass(valueType1, valueType2);

        String literal1 = value1.toString();
        String literal2 = value2.toString();

        QueryDataType type1 = QueryDataTypeUtils.resolveTypeForClass(value1.getClass());
        QueryDataType type2 = QueryDataTypeUtils.resolveTypeForClass(value2.getClass());

        SqlColumnType publicType1 = type1.getTypeFamily().getPublicType();
        SqlColumnType publicType2 = type2.getTypeFamily().getPublicType();

        int precedence1 = type1.getTypeFamily().getPrecedence();
        int precedence2 = type2.getTypeFamily().getPrecedence();

        // Column/column
        putCheckCommute(ExpressionBiValue.createBiValue(biClass, value1, value2), "field1", "field2", res);
        putCheckCommute(ExpressionBiValue.createBiValue(biClass, value1, null), "field1", "field2", RES_NULL);
        putCheckCommute(ExpressionBiValue.createBiValue(biClass, null, value2), "field1", "field2", RES_NULL);

        // Column/literal
        putCheckCommute(ExpressionValue.create(class1, value1), "field1", literal2, res);
        putCheckCommute(ExpressionValue.create(class2, value2), literal1, "field1", res);

        // Column/parameter
        if (precedence1 >= precedence2) {
            putCheckCommute(ExpressionValue.create(class1, value1), "field1", "?", res, value2);
        } else if (precedence1 >= QueryDataType.BIGINT.getTypeFamily().getPrecedence()) {
            putAndCheckFailure(ExpressionValue.create(class1, value1), sql(mode.token(), "field1", "?"), SqlErrorCode.DATA_EXCEPTION, parameterError(0, publicType1, publicType2), value2);
        }

        if (precedence2 >= precedence1) {
            putCheckCommute(ExpressionValue.create(class2, value2), "?", "field1", res, value1);
        } else if (precedence2 >= QueryDataType.BIGINT.getTypeFamily().getPrecedence()) {
            putAndCheckFailure(ExpressionValue.create(class2, value2), sql(mode.token(), "?", "field1"), SqlErrorCode.DATA_EXCEPTION, parameterError(0, publicType2, publicType1), value1);
        }

        // Literal/literal
        checkCommute(literal1, literal2, res);
    }

    private static Integer compareNumeric(Object value1, Object value2) {
        if (value1 == null || value2 == null) {
            return RES_NULL;
        }

        BigDecimal decimal1 = new BigDecimal(value1.toString());
        BigDecimal decimal2 = new BigDecimal(value2.toString());

        return decimal1.compareTo(decimal2);
    }

    private void checkUnsupportedColumnColumn(ExpressionType<?> type, ExpressionType<?>... excludeTypes) {
        for (ExpressionType<?> expressionType : excludeTypes) {
            ExpressionBiValue value = ExpressionBiValue.createBiValue(
                    ExpressionBiValue.createBiClass(type, expressionType),
                    null,
                    null
            );

            put(value);

            String sql = sql(mode.token(), "field1", "field2");

            String errorMessage = signatureErrorOperator(
                    mode.token(),
                    type.getFieldConverterType().getTypeFamily().getPublicType(),
                    expressionType.getFieldConverterType().getTypeFamily().getPublicType()
            );

            checkFailure0(sql, SqlErrorCode.PARSING, errorMessage);
        }
    }

    private void checkUnsupportedColumnLiteral(Object columnValue, SqlColumnType columnType, Literal... literals) {
        for (Literal literal : literals) {
            put(columnValue);

            String sql = sql(mode.token(), "this", literal.value);

            String errorMessage = signatureErrorOperator(
                    mode.token(),
                    columnType,
                    literal.type
            );

            checkFailure0(sql, SqlErrorCode.PARSING, errorMessage);
        }
    }

    private void checkUnsupportedColumnParameter(Object columnValue, SqlColumnType columnType, int paramPos, ExpressionType<?>... parameterTypes) {
        for (ExpressionType<?> parameterType : parameterTypes) {
            put(columnValue);

            String sql = sql(mode.token(), "this", "?");

            String errorMessage = parameterError(
                    paramPos,
                    columnType,
                    parameterType.getFieldConverterType().getTypeFamily().getPublicType()
            );

            try {
                checkFailure0(sql, SqlErrorCode.DATA_EXCEPTION, errorMessage, parameterType.valueFrom());
            } catch (AssertionError e) {
                throw new AssertionError("When comparing " + columnType + " & " + parameterType + ": " + e, e);
            }
        }
    }

    private void checkUnsupportedLiteralLiteral(String literalValue, SqlColumnType literalType, Literal... literals) {
        for (Literal literal : literals) {
            put(1);

            String sql = sql(mode.token(), literalValue, literal.value);

            String errorMessage = signatureErrorOperator(
                    mode.token(),
                    literalType,
                    literal.type
            );

            checkFailure0(sql, SqlErrorCode.PARSING, errorMessage);
        }
    }

    private void putCheckCommute(Object value, String operand1, String operand2, Integer expectedResult, Object... params) {
        put(value);

        checkCommute(operand1, operand2, expectedResult, params);
    }

    private void checkCommute(String operand1, String operand2, Integer expectedResult, Object... params) {
        check(operand1, operand2, expectedResult, params);
        check(operand2, operand1, inverse(expectedResult), params);
    }

    private void check(
            String operand1,
            String operand2,
            Integer expectedRes,
            Object... params
    ) {
        Boolean expectedValue = compare(expectedRes);

        String sql = sql(mode.token(), operand1, operand2);

        checkValue0(sql, SqlColumnType.BOOLEAN, expectedValue, params);
    }

    private void checkFailure(
            String operand1,
            String operand2,
            int expectedErrorCode,
            String expectedErrorMessage,
            Object... params
    ) {
        String sql = sql(mode.token(), operand1, operand2);

        checkFailure0(sql, expectedErrorCode, expectedErrorMessage, params);
    }

    private String sql(String token, String operand1, String operand2) {
        return "SELECT " + operand1 + " " + token + " " + operand2 + " FROM map";
    }

    private static Integer inverse(Integer result) {
        if (result == null) {
            return null;
        }

        switch (result) {
            case RES_GT:
                return RES_LT;

            case RES_LT:
                return RES_GT;

            default:
                assert result == RES_EQ;

                return RES_EQ;
        }
    }

    public Boolean compare(Integer res) {
        if (res == null) {
            return null;
        }

        switch (mode) {
            case EQ:
                return res == 0;

            case NEQ:
                return res != 0;

            case LT:
                return res < 0;

            case LTE:
                return res <= 0;

            case GT:
                return res > 0;

            default:
                assert mode == Mode.GTE;

                return res >= 0;
        }
    }

    private enum Mode {
        EQ("="),
        NEQ("<>"),
        LT("<"),
        LTE("<="),
        GT(">"),
        GTE(">=");

        private final String token;

        Mode(String token) {
            this.token = token;
        }

        private String token() {
            return token;
        }
    }

    private static class Literal {
        private final String value;
        private final SqlColumnType type;

        private Literal(String value, SqlColumnType type) {
            this.value = value;
            this.type = type;
        }
    }

    static class ComparableImpl implements Comparable<ComparableImpl>, Serializable {
        int innerField;

        ComparableImpl(int innerField) {
            this.innerField = innerField;
        }

        @Override
        public int compareTo(ComparableImpl that) {
            return Integer.compare(this.innerField, that.innerField);
        }
    }

    static class ComparableImpl2 implements Comparable<ComparableImpl2>, Serializable {
        int innerField;

        ComparableImpl2(int innerField) {
            this.innerField = innerField;
        }

        @Override
        public int compareTo(ComparableImpl2 that) {
            return Integer.compare(this.innerField, that.innerField);
        }
    }

    static class NonComparable implements Serializable {
    }
}
