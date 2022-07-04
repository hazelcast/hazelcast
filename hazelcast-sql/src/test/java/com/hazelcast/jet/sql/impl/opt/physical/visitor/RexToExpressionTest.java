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

package com.hazelcast.jet.sql.impl.opt.physical.visitor;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.RangeSet;
import com.hazelcast.jet.sql.impl.expression.Range;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeFactory;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.type.SqlDaySecondInterval;
import com.hazelcast.sql.impl.type.SqlYearMonthInterval;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.Sarg;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.junit.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import static org.apache.calcite.avatica.util.TimeUnit.DAY;
import static org.apache.calcite.avatica.util.TimeUnit.MONTH;
import static org.apache.calcite.avatica.util.TimeUnit.SECOND;
import static org.apache.calcite.avatica.util.TimeUnit.YEAR;
import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings({"unchecked", "UnstableApiUsage"})
public class RexToExpressionTest {

    private static final RelDataTypeFactory FACTORY = HazelcastTypeFactory.INSTANCE;
    private static final RexBuilder BUILDER = new RexBuilder(FACTORY);

    @Test
    public void test_boolean() {
        RexLiteral literal = literal(false, true, SqlTypeName.BOOLEAN);
        Range<?> converted = convert(literal);
        assertThat(converted).isEqualToComparingFieldByField(range(false, true));
    }

    @Test
    public void test_tinyint() {
        RexLiteral literal = literal(new BigDecimal(1), new BigDecimal(2), SqlTypeName.TINYINT);
        Range<?> converted = convert(literal);
        assertThat(converted).isEqualToComparingFieldByField(range((byte) 1, (byte) 2));
    }

    @Test
    public void test_smallint() {
        RexLiteral literal = literal(new BigDecimal(1), new BigDecimal(2), SqlTypeName.SMALLINT);
        Range<?> converted = convert(literal);
        assertThat(converted).isEqualToComparingFieldByField(range((short) 1, (short) 2));
    }

    @Test
    public void test_int() {
        RexLiteral literal = literal(new BigDecimal(1), new BigDecimal(2), SqlTypeName.INTEGER);
        Range<?> converted = convert(literal);
        assertThat(converted).isEqualToComparingFieldByField(range(1, 2));
    }

    @Test
    public void test_bigint() {
        RexLiteral literal = literal(new BigDecimal(1), new BigDecimal(2), SqlTypeName.BIGINT);
        Range<?> converted = convert(literal);
        assertThat(converted).isEqualToComparingFieldByField(range(1L, 2L));
    }

    @Test
    public void test_decimal() {
        RexLiteral literal = literal(new BigDecimal(1), new BigDecimal(2), SqlTypeName.DECIMAL);
        Range<?> converted = convert(literal);
        assertThat(converted).isEqualToComparingFieldByField(range(new BigDecimal(1), new BigDecimal(2)));
    }

    @Test
    public void test_real() {
        RexLiteral literal = literal(new BigDecimal(1), new BigDecimal(2), SqlTypeName.REAL);
        Range<?> converted = convert(literal);
        assertThat(converted).isEqualToComparingFieldByField(range(1F, 2F));
    }

    @Test
    public void test_double() {
        RexLiteral literal = literal(new BigDecimal(1), new BigDecimal(2), SqlTypeName.DOUBLE);
        Range<?> converted = convert(literal);
        assertThat(converted).isEqualToComparingFieldByField(range(1D, 2D));
    }

    @Test
    public void test_time() {
        RexLiteral literal = literal(
                new TimeString("12:23:34"),
                new TimeString("12:23:35"),
                SqlTypeName.TIME
        );
        Range<?> converted = convert(literal);
        assertThat(converted).isEqualToComparingFieldByField(range(
                LocalTime.of(12, 23, 34),
                LocalTime.of(12, 23, 35))
        );
    }

    @Test
    public void test_date() {
        RexLiteral literal = literal(
                new DateString("2021-09-17"),
                new DateString("2021-09-18"),
                SqlTypeName.DATE
        );
        Range<?> converted = convert(literal);
        assertThat(converted).isEqualToComparingFieldByField(range(
                LocalDate.of(2021, 9, 17),
                LocalDate.of(2021, 9, 18))
        );
    }

    @Test
    public void test_timestamp() {
        RexLiteral literal = literal(
                new TimestampString("2021-09-17 12:23:34"),
                new TimestampString("2021-09-17 12:23:35"),
                SqlTypeName.TIMESTAMP
        );
        Range<?> converted = convert(literal);
        assertThat(converted).isEqualToComparingFieldByField(range(
                LocalDateTime.of(2021, 9, 17, 12, 23, 34),
                LocalDateTime.of(2021, 9, 17, 12, 23, 35))
        );
    }

    @Test
    public void test_intervalYearMonth() {
        RelDataType type = FACTORY.createSqlIntervalType(new SqlIntervalQualifier(YEAR, MONTH, SqlParserPos.ZERO));
        RexLiteral literal = BUILDER.makeSearchArgumentLiteral(sarg(new BigDecimal(1), new BigDecimal(2)), type);
        Range<?> converted = convert(literal);
        assertThat(converted).isEqualToComparingFieldByField(range(
                new SqlYearMonthInterval(1),
                new SqlYearMonthInterval(2))
        );
    }

    @Test
    public void test_intervalDaySecond() {
        RelDataType type = FACTORY.createSqlIntervalType(new SqlIntervalQualifier(DAY, SECOND, SqlParserPos.ZERO));
        RexLiteral literal = BUILDER.makeSearchArgumentLiteral(sarg(new BigDecimal(1), new BigDecimal(2)), type);
        Range<?> converted = convert(literal);
        assertThat(converted).isEqualToComparingFieldByField(range(
                new SqlDaySecondInterval(1),
                new SqlDaySecondInterval(2))
        );
    }

    private static <C extends Comparable<C>> Range<C> convert(RexLiteral literal) {
        Expression<?> expression = RexToExpression.convertLiteral(literal);
        return (Range<C>) expression.eval(null, null);
    }

    private static <C extends Comparable<C>> RexLiteral literal(C left, C right, SqlTypeName typeName) {
        Sarg<C> sarg = sarg(left, right);
        RelDataType type = HazelcastTypeUtils.createType(FACTORY, typeName, true);
        return BUILDER.makeSearchArgumentLiteral(sarg, type);
    }

    private static <C extends Comparable<C>> Sarg<C> sarg(C left, C right) {
        return Sarg.of(RexUnknownAs.UNKNOWN, rangeSet(left, right));
    }

    private static <C extends Comparable<C>> RangeSet<C> rangeSet(C left, C right) {
        return ImmutableRangeSet.of(com.google.common.collect.Range.closed(left, right));
    }

    private static <C extends Comparable<C>> Range<C> range(C left, C right) {
        return new Range<>(rangeSet(left, right));
    }
}
