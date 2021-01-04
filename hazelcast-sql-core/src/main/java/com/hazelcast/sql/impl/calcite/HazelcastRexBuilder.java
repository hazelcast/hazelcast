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

package com.hazelcast.sql.impl.calcite;

import com.hazelcast.sql.impl.calcite.validate.types.HazelcastIntegerType;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeFactory;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeUtils;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import com.hazelcast.sql.impl.type.converter.Converter;
import com.hazelcast.sql.impl.type.converter.Converters;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.util.Locale;

import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static org.apache.calcite.sql.type.SqlTypeName.ANY;
import static org.apache.calcite.sql.type.SqlTypeName.DATE;
import static org.apache.calcite.sql.type.SqlTypeName.TIME;
import static org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP;

/**
 * Custom Hazelcast expression builder.
 * <p>
 * Currently, this custom expression builder is used just to workaround quirks
 * of the default Calcite expression builder.
 */
public final class HazelcastRexBuilder extends RexBuilder {
    // a format just like ISO_LOCAL_TIME, but seconds are not optional as calcite requires
    private static final DateTimeFormatter TIME_FORMATTER = new DateTimeFormatterBuilder()
            .appendValue(HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 2)
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 2)
            .optionalStart()
            .appendFraction(NANO_OF_SECOND, 0, 9, true)
            .toFormatter(Locale.US)
            .withResolverStyle(ResolverStyle.STRICT);

    // a format just like ISO_LOCAL_DATE_TIME, but instead of `T` a space is used. Time format is the above.
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .append(TIME_FORMATTER)
            .toFormatter()
            .withChronology(IsoChronology.INSTANCE)
            .withResolverStyle(ResolverStyle.STRICT);

    public HazelcastRexBuilder(HazelcastTypeFactory typeFactory) {
        super(typeFactory);
    }

    @Override
    public RexNode makeLiteral(Object value, RelDataType type, boolean allowCast) {
        // Make sure that numeric literals get a correct return type during the conversion.
        // Without this code, Apache Calcite may assign incorrect types to some literals during conversion.
        // For example, new BigDecimal(Long.MAX_VALUE + "1") will receive the BIGINT type.
        // To see the problem in action, you may comment out this code and run CastFunctionIntegrationTest.
        // Some conversions will fail due to precision loss.

        if (type.getSqlTypeName() == ANY && value instanceof Number) {
            Converter converter = Converters.getConverter(value.getClass());

            if (converter != null) {
                QueryDataTypeFamily typeFamily = converter.getTypeFamily();

                if (typeFamily.isNumericInteger()) {
                    int bitWidth = HazelcastIntegerType.bitWidthOf(((Number) value).longValue());
                    type = HazelcastIntegerType.create(bitWidth, false);
                } else {
                    SqlTypeName typeName = HazelcastTypeUtils.toCalciteType(typeFamily);

                    type = HazelcastTypeFactory.INSTANCE.createSqlType(typeName);
                }
            }
        }

        if (type.getSqlTypeName() == TIME && value instanceof LocalTime) {
            // We convert to Calcite's TimeString. We could also convert to an Integer with millisOfDay, but
            // that can't contain nanoseconds, only the string can.
            value = new TimeString(((LocalTime) value).format(TIME_FORMATTER));
        }

        if (type.getSqlTypeName() == DATE && value instanceof LocalDate) {
            value = new DateString(value.toString());
        }

        if (type.getSqlTypeName() == TIMESTAMP && value instanceof LocalDateTime) {

            value = new TimestampString(((LocalDateTime) value).format(TIMESTAMP_FORMATTER));
        }

        return super.makeLiteral(value, type, allowCast);
    }

    @Override
    public RexNode makeCast(RelDataType type, RexNode exp, boolean matchNullability) {
        // Calcite converts `CAST(booleanExpr AS <exactIntegerType>)` to `CASE WHEN booleanExpr THEN 1 ELSE 0`.
        // We want to leave it just as a cast.
        if (!(exp instanceof RexLiteral) && exp.getType().getSqlTypeName() == SqlTypeName.BOOLEAN
                && SqlTypeUtil.isExactNumeric(type)) {
            return makeAbstractCast(type, exp);
        }

        return super.makeCast(type, exp, matchNullability);
    }
}
