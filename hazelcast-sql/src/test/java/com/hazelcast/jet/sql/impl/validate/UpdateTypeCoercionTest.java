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

package com.hazelcast.jet.sql.impl.validate;

import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.support.expressions.ExpressionValue;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.regex.Pattern;

import static com.hazelcast.sql.impl.type.QueryDataType.BIGINT;
import static com.hazelcast.sql.impl.type.QueryDataType.BOOLEAN;
import static com.hazelcast.sql.impl.type.QueryDataType.DATE;
import static com.hazelcast.sql.impl.type.QueryDataType.DECIMAL;
import static com.hazelcast.sql.impl.type.QueryDataType.DOUBLE;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static com.hazelcast.sql.impl.type.QueryDataType.NULL;
import static com.hazelcast.sql.impl.type.QueryDataType.OBJECT;
import static com.hazelcast.sql.impl.type.QueryDataType.REAL;
import static com.hazelcast.sql.impl.type.QueryDataType.SMALLINT;
import static com.hazelcast.sql.impl.type.QueryDataType.TIME;
import static com.hazelcast.sql.impl.type.QueryDataType.TIMESTAMP;
import static com.hazelcast.sql.impl.type.QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME;
import static com.hazelcast.sql.impl.type.QueryDataType.TINYINT;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;
import static com.hazelcast.sql.impl.type.converter.AbstractTemporalConverter.DEFAULT_ZONE;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
public class UpdateTypeCoercionTest extends SqlTestSupport {

    private final SqlService sqlService = instance().getSql();

    @Parameter
    public TestParams testParams;

    @SuppressWarnings({"checkstyle:LineLength", "checkstyle:MethodLength"})
    @Parameters(name = "{0}")
    public static Object[] parameters() {
        return new Object[]{
                // NULL
                TestParams.passingCase(1001, NULL, VARCHAR, "null", null, null),
                TestParams.passingCase(1002, NULL, BOOLEAN, "null", null, null),
                TestParams.passingCase(1003, NULL, TINYINT, "null", null, null),
                TestParams.passingCase(1004, NULL, SMALLINT, "null", null, null),
                TestParams.passingCase(1005, NULL, INT, "null", null, null),
                TestParams.passingCase(1006, NULL, BIGINT, "null", null, null),
                TestParams.passingCase(1007, NULL, DECIMAL, "null", null, null),
                TestParams.passingCase(1008, NULL, REAL, "null", null, null),
                TestParams.passingCase(1009, NULL, DOUBLE, "null", null, null),
                TestParams.passingCase(1010, NULL, TIME, "null", null, null),
                TestParams.passingCase(1011, NULL, DATE, "null", null, null),
                TestParams.passingCase(1012, NULL, TIMESTAMP, "null", null, null),
                TestParams.passingCase(1013, NULL, TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, null, null, null),
                TestParams.passingCase(1014, NULL, OBJECT, "null", null, null),

                // VARCHAR
                TestParams.passingCase(1101, VARCHAR, VARCHAR, "'foo'", "foo", "foo"),
                TestParams.failingCase(1102, VARCHAR, BOOLEAN, "'true'", "true",
                        "Cannot assign to target field '.+' of type BOOLEAN from source field '.+' of type VARCHAR"),
                TestParams.failingCase(1103, VARCHAR, TINYINT, "'42'", "42",
                        "Cannot assign to target field '.+' of type TINYINT from source field '.+' of type VARCHAR"),
                TestParams.failingCase(1104, VARCHAR, TINYINT, "'420'", "420",
                        "Cannot assign to target field '.+' of type TINYINT from source field '.+' of type VARCHAR"),
                TestParams.failingCase(1105, VARCHAR, TINYINT, "'foo'", "foo",
                        "Cannot assign to target field '.+' of type TINYINT from source field '.+' of type VARCHAR"),
                TestParams.failingCase(1106, VARCHAR, SMALLINT, "'42'", "42",
                        "Cannot assign to target field '.+' of type SMALLINT from source field '.+' of type VARCHAR"),
                TestParams.failingCase(1107, VARCHAR, SMALLINT, "'42000'", "42000",
                        "Cannot assign to target field '.+' of type SMALLINT from source field '.+' of type VARCHAR"),
                TestParams.failingCase(1108, VARCHAR, SMALLINT, "'foo'", "foo",
                        "Cannot assign to target field '.+' of type SMALLINT from source field '.+' of type VARCHAR"),
                TestParams.failingCase(1109, VARCHAR, INT, "'42'", "42",
                        "Cannot assign to target field '.+' of type INTEGER from source field '.+' of type VARCHAR"),
                TestParams.failingCase(1110, VARCHAR, INT, "'4200000000'", "4200000000",
                        "Cannot assign to target field '.+' of type INTEGER from source field '.+' of type VARCHAR"),
                TestParams.failingCase(1111, VARCHAR, INT, "'foo'", "foo",
                        "Cannot assign to target field '.+' of type INTEGER from source field '.+' of type VARCHAR"),
                TestParams.failingCase(1112, VARCHAR, BIGINT, "'42'", "42",
                        "Cannot assign to target field '.+' of type BIGINT from source field '.+' of type VARCHAR"),
                TestParams.failingCase(1113, VARCHAR, BIGINT, "'9223372036854775808000'", "9223372036854775808000",
                        "Cannot assign to target field '.+' of type BIGINT from source field '.+' of type VARCHAR"),
                TestParams.failingCase(1114, VARCHAR, BIGINT, "'foo'", "foo",
                        "Cannot assign to target field '.+' of type BIGINT from source field '.+' of type VARCHAR"),
                TestParams.failingCase(1115, VARCHAR, DECIMAL, "'1.5'", "1.5",
                        "Cannot assign to target field '.+' of type DECIMAL\\(38, 38\\) from source field '.+' of type VARCHAR"),
                TestParams.failingCase(1116, VARCHAR, DECIMAL, "'foo'", "foo",
                        "Cannot assign to target field '.+' of type DECIMAL\\(38, 38\\) from source field '.+' of type VARCHAR"),
                TestParams.failingCase(1117, VARCHAR, REAL, "'1.5'", "1.5",
                        "Cannot assign to target field '.+' of type REAL from source field '.+' of type VARCHAR"),
                TestParams.failingCase(1118, VARCHAR, REAL, "'foo'", "foo",
                        "Cannot assign to target field '.+' of type REAL from source field '.+' of type VARCHAR"),
                TestParams.failingCase(1119, VARCHAR, DOUBLE, "'1.5'", "1.5",
                        "Cannot assign to target field '.+' of type DOUBLE from source field '.+' of type VARCHAR"),
                TestParams.failingCase(1120, VARCHAR, DOUBLE, "'foo'", "foo",
                        "Cannot assign to target field '.+' of type DOUBLE from source field '.+' of type VARCHAR"),
                TestParams.passingCase(1121, VARCHAR, TIME, "'01:42:01'", "01:42:01", LocalTime.of(1, 42, 1))
                        .setExpectedFailureNonLiteralRegex("Cannot assign to target field '.+' of type TIME from source field '.+' of type VARCHAR"),
                TestParams.failingCase(1122, VARCHAR, TIME, "'foo'", "foo",
                        "Cannot parse VARCHAR value to TIME")
                        .setExpectedFailureNonLiteralRegex("Cannot assign to target field '.+' of type TIME from source field '.+' of type VARCHAR"),
                TestParams.passingCase(1123, VARCHAR, DATE, "'2020-12-30'", "2020-12-30", LocalDate.of(2020, 12, 30))
                        .setExpectedFailureNonLiteralRegex("Cannot assign to target field '.+' of type DATE from source field '.+' of type VARCHAR"),
                TestParams.failingCase(1124, VARCHAR, DATE, "'foo'", "foo",
                        "Cannot parse VARCHAR value to DATE")
                        .setExpectedFailureNonLiteralRegex("Cannot assign to target field '.+' of type DATE from source field '.+' of type VARCHAR"),
                TestParams.passingCase(1125, VARCHAR, TIMESTAMP, "'2020-12-30T01:42:00'", "2020-12-30T01:42:00",
                        LocalDateTime.of(2020, 12, 30, 1, 42))
                        .setExpectedFailureNonLiteralRegex("Cannot assign to target field '.+' of type TIMESTAMP from source field '.+' of type VARCHAR"),
                TestParams.failingCase(1126, VARCHAR, TIMESTAMP, "'foo'", "foo",
                        "Cannot parse VARCHAR value to TIMESTAMP")
                        .setExpectedFailureNonLiteralRegex("Cannot assign to target field '.+' of type TIMESTAMP from source field '.+' of type VARCHAR"),
                TestParams.passingCase(1127, VARCHAR, TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, "'2020-12-30T01:42:00-05:00'",
                        "2020-12-30T01:42:00-05:00", OffsetDateTime.of(2020, 12, 30, 1, 42, 0, 0, ZoneOffset.ofHours(-5)))
                        .setExpectedFailureNonLiteralRegex("Cannot assign to target field '.+' of type TIMESTAMP WITH TIME ZONE from source field '.+' of type VARCHAR"),
                TestParams.failingCase(1128, VARCHAR, TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, "'foo'", "foo",
                        "Cannot parse VARCHAR value to TIMESTAMP_WITH_TIME_ZONE")
                        .setExpectedFailureNonLiteralRegex("Cannot assign to target field '.+' of type TIMESTAMP WITH TIME ZONE from source field '.+' of type VARCHAR"),
                TestParams.passingCase(1129, VARCHAR, OBJECT, "'foo'", "foo", "foo"),

                // BOOLEAN
                TestParams.failingCase(1201, BOOLEAN, VARCHAR, "true", "true",
                        "Cannot assign to target field '.+' of type VARCHAR from source field '.+' of type BOOLEAN"),
                TestParams.passingCase(1202, BOOLEAN, BOOLEAN, "true", "true", true),
                TestParams.failingCase(1203, BOOLEAN, TINYINT, "true", "true",
                        "Cannot assign to target field '.+' of type TINYINT from source field 'EXPR\\$0' of type BOOLEAN"),
                TestParams.failingCase(1204, BOOLEAN, SMALLINT, "true", "true",
                        "Cannot assign to target field '.+' of type SMALLINT from source field 'EXPR\\$0' of type BOOLEAN"),
                TestParams.failingCase(1205, BOOLEAN, INT, "true", "true",
                        "Cannot assign to target field '.+' of type INTEGER from source field 'EXPR\\$0' of type BOOLEAN"),
                TestParams.failingCase(1206, BOOLEAN, BIGINT, "true", "true",
                        "Cannot assign to target field '.+' of type BIGINT from source field 'EXPR\\$0' of type BOOLEAN"),
                TestParams.failingCase(1207, BOOLEAN, DECIMAL, "true", "true",
                        "Cannot assign to target field '.+' of type DECIMAL\\(38, 38\\) from source field 'EXPR\\$0' of type BOOLEAN"),
                TestParams.failingCase(1208, BOOLEAN, REAL, "true", "true",
                        "Cannot assign to target field '.+' of type REAL from source field 'EXPR\\$0' of type BOOLEAN"),
                TestParams.failingCase(1209, BOOLEAN, DOUBLE, "true", "true",
                        "Cannot assign to target field '.+' of type DOUBLE from source field 'EXPR\\$0' of type BOOLEAN"),
                TestParams.failingCase(1210, BOOLEAN, TIME, "true", "true",
                        "Cannot assign to target field '.+' of type TIME from source field '.+' of type BOOLEAN"),
                TestParams.failingCase(1211, BOOLEAN, DATE, "true", "true",
                        "Cannot assign to target field '.+' of type DATE from source field '.+' of type BOOLEAN"),
                TestParams.failingCase(1212, BOOLEAN, TIMESTAMP, "true", "true",
                        "Cannot assign to target field '.+' of type TIMESTAMP from source field '.+' of type BOOLEAN"),
                TestParams.failingCase(1213, BOOLEAN, TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, "true", "true",
                        "Cannot assign to target field '.+' of type TIMESTAMP WITH TIME ZONE from source field '.+' of type BOOLEAN"),
                TestParams.passingCase(1214, BOOLEAN, OBJECT, "true", "true", true),

                // TINYINT
                TestParams.failingCase(1301, TINYINT, VARCHAR, "cast(42 as tinyint)", "42",
                        "Cannot assign to target field '.+' of type VARCHAR from source field '.+' of type TINYINT"),
                TestParams.failingCase(1302, TINYINT, BOOLEAN, "cast(42 as tinyint)", "42",
                        "Cannot assign to target field '.+' of type BOOLEAN from source field '.+' of type TINYINT"),
                TestParams.passingCase(1303, TINYINT, TINYINT, "cast(42 as tinyint)", "42", (byte) 42),
                TestParams.passingCase(1304, TINYINT, SMALLINT, "cast(42 as tinyint)", "42", (short) 42),
                TestParams.passingCase(1305, TINYINT, INT, "cast(42 as tinyint)", "42", 42),
                TestParams.passingCase(1306, TINYINT, BIGINT, "cast(42 as tinyint)", "42", 42L),
                TestParams.passingCase(1307, TINYINT, DECIMAL, "cast(42 as tinyint)", "42", BigDecimal.valueOf(42)),
                TestParams.passingCase(1308, TINYINT, REAL, "cast(42 as tinyint)", "42", 42f),
                TestParams.passingCase(1309, TINYINT, DOUBLE, "cast(42 as tinyint)", "42", 42d),
                TestParams.failingCase(1310, TINYINT, TIME, "cast(42 as tinyint)",
                        "42", "Cannot assign to target field '.+' of type TIME from source field '.+' of type TINYINT"),
                TestParams.failingCase(1311, TINYINT, DATE, "cast(42 as tinyint)",
                        "42", "Cannot assign to target field '.+' of type DATE from source field '.+' of type TINYINT"),
                TestParams.failingCase(1312, TINYINT, TIMESTAMP, "cast(42 as tinyint)",
                        "42", "Cannot assign to target field '.+' of type TIMESTAMP from source field '.+' of type TINYINT"),
                TestParams.failingCase(1313, TINYINT, TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, "cast(42 as tinyint)",
                        "42", "Cannot assign to target field '.+' of type TIMESTAMP WITH TIME ZONE from source field '.+' of type TINYINT"),
                TestParams.passingCase(1314, TINYINT, OBJECT, "cast(42 as tinyint)", "42", (byte) 42),

                // SMALLINT
                TestParams.failingCase(1401, SMALLINT, VARCHAR, "cast(42 as smallint)", "42",
                        "Cannot assign to target field '.+' of type VARCHAR from source field '.+' of type SMALLINT"),
                TestParams.failingCase(1402, SMALLINT, BOOLEAN, "cast(42 as smallint)",
                        "42", "Cannot assign to target field '.+' of type BOOLEAN from source field '.+' of type SMALLINT"),
                TestParams.passingCase(1403, SMALLINT, TINYINT, "cast(42 as smallint)", "42", (byte) 42),
                TestParams.failingCase(1404, SMALLINT, TINYINT, "420",
                        "420", "Numeric overflow while converting SMALLINT to TINYINT"),
                TestParams.passingCase(1405, SMALLINT, SMALLINT, "cast(42 as smallint)", "42", (short) 42),
                TestParams.passingCase(1406, SMALLINT, INT, "cast(42 as smallint)", "42", 42),
                TestParams.passingCase(1407, SMALLINT, BIGINT, "cast(42 as smallint)", "42", 42L),
                TestParams.passingCase(1408, SMALLINT, DECIMAL, "cast(42 as smallint)", "42", BigDecimal.valueOf(42)),
                TestParams.passingCase(1409, SMALLINT, REAL, "cast(42 as smallint)", "42", 42f),
                TestParams.passingCase(1410, SMALLINT, DOUBLE, "cast(42 as smallint)", "42", 42d),
                TestParams.failingCase(1411, SMALLINT, TIME, "cast(42 as smallint)",
                        "42", "Cannot assign to target field '.+' of type TIME from source field '.+' of type SMALLINT"),
                TestParams.failingCase(1412, SMALLINT, DATE, "cast(42 as smallint)",
                        "42", "Cannot assign to target field '.+' of type DATE from source field '.+' of type SMALLINT"),
                TestParams.failingCase(1413, SMALLINT, TIMESTAMP, "cast(42 as smallint)",
                        "42", "Cannot assign to target field '.+' of type TIMESTAMP from source field '.+' of type SMALLINT"),
                TestParams.failingCase(1414, SMALLINT, TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, "cast(42 as smallint)",
                        "42", "Cannot assign to target field '.+' of type TIMESTAMP WITH TIME ZONE from source field '.+' of type SMALLINT"),
                TestParams.passingCase(1415, SMALLINT, OBJECT, "cast(42 as smallint)", "42", (short) 42),

                // INTEGER
                TestParams.failingCase(1501, INT, VARCHAR, "cast(42 as integer)", "42",
                        "Cannot assign to target field '.+' of type VARCHAR from source field '.+' of type INTEGER"),
                TestParams.failingCase(1502, INT, BOOLEAN, "cast(42 as integer)",
                        "42", "Cannot assign to target field '.+' of type BOOLEAN from source field '.+' of type INTEGER"),
                TestParams.passingCase(1503, INT, TINYINT, "cast(42 as integer)", "42", (byte) 42),
                TestParams.failingCase(1504, INT, TINYINT, "42000", "42000",
                        "Numeric overflow while converting INTEGER to TINYINT"),
                TestParams.passingCase(1505, INT, SMALLINT, "cast(42 as integer)", "42", (short) 42),
                TestParams.failingCase(1506, INT, SMALLINT, "42000", "42000",
                        "Numeric overflow while converting INTEGER to SMALLINT"),
                TestParams.passingCase(1507, INT, INT, "cast(42 as integer)", "42", 42),
                TestParams.passingCase(1508, INT, BIGINT, "cast(42 as integer)", "42", 42L),
                TestParams.passingCase(1509, INT, DECIMAL, "cast(42 as integer)", "42", BigDecimal.valueOf(42)),
                TestParams.passingCase(1510, INT, REAL, "cast(42 as integer)", "42", 42f),
                TestParams.passingCase(1511, INT, DOUBLE, "cast(42 as integer)", "42", 42d),
                TestParams.failingCase(1512, INT, TIME, "cast(42 as integer)",
                        "42", "Cannot assign to target field '.+' of type TIME from source field '.+' of type INTEGER"),
                TestParams.failingCase(1513, INT, DATE, "cast(42 as integer)",
                        "42", "Cannot assign to target field '.+' of type DATE from source field '.+' of type INTEGER"),
                TestParams.failingCase(1514, INT, TIMESTAMP, "cast(42 as integer)",
                        "42", "Cannot assign to target field '.+' of type TIMESTAMP from source field '.+' of type INTEGER"),
                TestParams.failingCase(1515, INT, TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, "cast(42 as integer)",
                        "42", "Cannot assign to target field '.+' of type TIMESTAMP WITH TIME ZONE from source field '.+' of type INTEGER"),
                TestParams.passingCase(1516, INT, OBJECT, "cast(42 as integer)", "42", 42),

                // BIGINT
                TestParams.failingCase(1601, BIGINT, VARCHAR, "cast(42 as bigint)", "42",
                        "Cannot assign to target field '.+' of type VARCHAR from source field '.+' of type BIGINT"),
                TestParams.failingCase(1602, BIGINT, BOOLEAN, "cast(42 as bigint)",
                        "42", "Cannot assign to target field '.+' of type BOOLEAN from source field '.+' of type BIGINT"),
                TestParams.passingCase(1603, BIGINT, TINYINT, "cast(42 as bigint)", "42", (byte) 42),
                TestParams.failingCase(1604, BIGINT, TINYINT, "4200000000", "4200000000",
                        "Numeric overflow while converting BIGINT to TINYINT"),
                TestParams.passingCase(1605, BIGINT, SMALLINT, "cast(42 as bigint)", "42", (short) 42),
                TestParams.failingCase(1606, BIGINT, SMALLINT, "4200000000", "4200000000",
                        "Numeric overflow while converting BIGINT to SMALLINT"),
                TestParams.passingCase(1607, BIGINT, INT, "cast(42 as bigint)", "42", 42),
                TestParams.failingCase(1608, BIGINT, INT, "4200000000", "4200000000",
                        "Numeric overflow while converting BIGINT to INTEGER"),
                TestParams.passingCase(1609, BIGINT, BIGINT, "cast(42 as bigint)", "42", 42L),
                TestParams.passingCase(1610, BIGINT, DECIMAL, "cast(42 as bigint)", "42", BigDecimal.valueOf(42)),
                TestParams.passingCase(1611, BIGINT, REAL, "cast(42 as bigint)", "42", 42f),
                TestParams.passingCase(1612, BIGINT, DOUBLE, "cast(42 as bigint)", "42", 42d),
                TestParams.failingCase(1613, BIGINT, TIME, "cast(42 as bigint)",
                        "42", "Cannot assign to target field '.+' of type TIME from source field '.+' of type BIGINT"),
                TestParams.failingCase(1614, BIGINT, DATE, "cast(42 as bigint)",
                        "42", "Cannot assign to target field '.+' of type DATE from source field '.+' of type BIGINT"),
                TestParams.failingCase(1615, BIGINT, TIMESTAMP, "cast(42 as bigint)",
                        "42", "Cannot assign to target field '.+' of type TIMESTAMP from source field '.+' of type BIGINT"),
                TestParams.failingCase(1616, BIGINT, TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, "cast(42 as bigint)",
                        "42", "Cannot assign to target field '.+' of type TIMESTAMP WITH TIME ZONE from source field '.+' of type BIGINT"),
                TestParams.passingCase(1617, BIGINT, OBJECT, "cast(42 as bigint)", "42", 42L),

                // DECIMAL
                TestParams.failingCase(1701, DECIMAL, VARCHAR, "cast(42 as decimal)", "42",
                        "Cannot assign to target field '.+' of type VARCHAR from source field '.+' of type DECIMAL\\(38, 38\\)"),
                TestParams.failingCase(1702, DECIMAL, BOOLEAN, "cast(42 as decimal)",
                        "42", "Cannot assign to target field '.+' of type BOOLEAN from source field '.+' of type DECIMAL\\(38, 38\\)"),
                TestParams.passingCase(1703, DECIMAL, TINYINT, "cast(42 as decimal)", "42", (byte) 42),
                TestParams.failingCase(1704, DECIMAL, TINYINT, "9223372036854775809", "9223372036854775809",
                        "Numeric overflow while converting DECIMAL to TINYINT"),
                TestParams.passingCase(1705, DECIMAL, SMALLINT, "cast(42 as decimal)", "42", (short) 42),
                TestParams.failingCase(1706, DECIMAL, SMALLINT, "9223372036854775809", "9223372036854775809",
                        "Numeric overflow while converting DECIMAL to SMALLINT"),
                TestParams.passingCase(1707, DECIMAL, INT, "cast(42 as decimal)", "42", 42),
                TestParams.failingCase(1708, DECIMAL, INT, "9223372036854775809", "9223372036854775809",
                        "Numeric overflow while converting DECIMAL to INTEGER"),
                TestParams.passingCase(1709, DECIMAL, BIGINT, "cast(42 as decimal)", "42", 42L),
                TestParams.passingCase(1710, DECIMAL, BIGINT, "cast(42.1 as decimal)", "42.1", 42L),
                TestParams.failingCase(1711, DECIMAL, BIGINT, "9223372036854775809", "9223372036854775809",
                        "Numeric overflow while converting DECIMAL to BIGINT"),
                TestParams.passingCase(1712, DECIMAL, DECIMAL, "cast(42 as decimal)", "42", BigDecimal.valueOf(42)),
                TestParams.passingCase(1713, DECIMAL, REAL, "cast(42 as decimal)", "42", 42f),
                TestParams.passingCase(1714, DECIMAL, DOUBLE, "cast(42 as decimal)", "42", 42d),
                TestParams.failingCase(1715, DECIMAL, TIME, "cast(42 as decimal)",
                        "42", "Cannot assign to target field '.+' of type TIME from source field '.+' of type DECIMAL"),
                TestParams.failingCase(1716, DECIMAL, DATE, "cast(42 as decimal)",
                        "42", "Cannot assign to target field '.+' of type DATE from source field '.+' of type DECIMAL"),
                TestParams.failingCase(1717, DECIMAL, TIMESTAMP, "cast(42 as decimal)",
                        "42", "Cannot assign to target field '.+' of type TIMESTAMP from source field '.+' of type DECIMAL\\(38, 38\\)"),
                TestParams.failingCase(1718, DECIMAL, TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, "cast(42 as decimal)",
                        "42", "Cannot assign to target field '.+' of type TIMESTAMP WITH TIME ZONE from source field '.+' of type DECIMAL\\(38, 38\\)"),
                TestParams.passingCase(1719, DECIMAL, OBJECT, "cast(42 as decimal)", "42", BigDecimal.valueOf(42)),

                // REAL
                TestParams.failingCase(1801, REAL, VARCHAR, "cast(42 as real)", "42",
                        "Cannot assign to target field '.+' of type VARCHAR from source field '.+' of type REAL"),
                TestParams.failingCase(1802, REAL, BOOLEAN, "cast(42 as real)",
                        "42", "Cannot assign to target field '.+' of type BOOLEAN from source field '.+' of type REAL"),
                TestParams.passingCase(1803, REAL, TINYINT, "cast(42 as real)", "42", (byte) 42),
                TestParams.failingCase(1804, REAL, TINYINT, "cast(420 as real)",
                        "420", "Numeric overflow while converting REAL to TINYINT"),
                TestParams.passingCase(1805, REAL, SMALLINT, "cast(42 as real)", "42", (short) 42),
                TestParams.failingCase(1806, REAL, SMALLINT, "cast(420000 as real)",
                        "420000", "Numeric overflow while converting REAL to SMALLINT"),
                TestParams.passingCase(1807, REAL, INT, "cast(42 as real)", "42", 42),
                TestParams.failingCase(1808, REAL, INT, "cast(4200000000 as real)",
                        "4200000000", "Numeric overflow while converting REAL to INTEGER"),
                TestParams.passingCase(1809, REAL, BIGINT, "cast(42 as real)", "42", 42L),
                TestParams.passingCase(18010, REAL, BIGINT, "cast(42.1 as real)", "42.1", 42L),
                TestParams.failingCase(1811, REAL, BIGINT, "cast(18223372036854775808000 as real)",
                        "18223372036854775808000", "Numeric overflow while converting REAL to BIGINT"),
                TestParams.passingCase(1812, REAL, DECIMAL, "cast(42 as real)", "42", BigDecimal.valueOf(42)),
                TestParams.passingCase(1813, REAL, REAL, "cast(42 as real)", "42", 42f),
                TestParams.passingCase(1814, REAL, DOUBLE, "cast(42 as real)", "42", 42d),
                TestParams.failingCase(1815, REAL, TIME, "cast(42 as real)",
                        "42", "Cannot assign to target field '.+' of type TIME from source field '.+' of type REAL"),
                TestParams.failingCase(1816, REAL, DATE, "cast(42 as real)",
                        "42", "Cannot assign to target field '.+' of type DATE from source field '.+' of type REAL"),
                TestParams.failingCase(1817, REAL, TIMESTAMP, "cast(42 as real)",
                        "42", "Cannot assign to target field '.+' of type TIMESTAMP from source field '.+' of type REAL"),
                TestParams.failingCase(1818, REAL, TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, "cast(42 as real)",
                        "42", "Cannot assign to target field '.+' of type TIMESTAMP WITH TIME ZONE from source field '.+' of type REAL"),
                TestParams.passingCase(1819, REAL, OBJECT, "cast(42 as real)", "42", 42f),

                // DOUBLE
                TestParams.failingCase(1901, DOUBLE, VARCHAR, "cast(42 as double)", "42",
                        "Cannot assign to target field '.+' of type VARCHAR from source field '.+' of type DOUBLE"),
                TestParams.failingCase(1902, DOUBLE, BOOLEAN, "cast(42 as double)",
                        "42", "Cannot assign to target field '.+' of type BOOLEAN from source field '.+' of type DOUBLE"),
                TestParams.passingCase(1903, DOUBLE, TINYINT, "cast(42 as double)", "42", (byte) 42),
                TestParams.failingCase(1904, DOUBLE, TINYINT, "cast(420 as double)",
                        "420", "Numeric overflow while converting DOUBLE to TINYINT"),
                TestParams.passingCase(1905, DOUBLE, SMALLINT, "cast(42 as double)", "42", (short) 42),
                TestParams.failingCase(1906, DOUBLE, SMALLINT, "cast(420000 as double)",
                        "420000", "Numeric overflow while converting DOUBLE to SMALLINT"),
                TestParams.passingCase(1907, DOUBLE, INT, "cast(42 as double)", "42", 42),
                TestParams.failingCase(1908, DOUBLE, INT, "cast(4200000000 as double)",
                        "4200000000", "Numeric overflow while converting DOUBLE to INTEGER"),
                TestParams.passingCase(1909, DOUBLE, BIGINT, "cast(42 as double)", "42", 42L),
                TestParams.failingCase(1910, DOUBLE, BIGINT, "cast(19223372036854775808000 as double)",
                        "19223372036854775808000", "Numeric overflow while converting DOUBLE to BIGINT"),
                TestParams.passingCase(1911, DOUBLE, DECIMAL, "cast(42 as double)", "42", BigDecimal.valueOf(42)),
                TestParams.passingCase(1912, DOUBLE, REAL, "cast(42 as double)", "42", 42f),
                TestParams.passingCase(1913, DOUBLE, BIGINT, "cast(42.1 as double)", "42.1", 42L),
                TestParams.failingCase(1914, DOUBLE, REAL, "cast(42e42 as double)", "42e42",
                        "Numeric overflow while converting DOUBLE to REAL"),
                TestParams.passingCase(1915, DOUBLE, DOUBLE, "cast(42 as double)", "42", 42d),
                TestParams.failingCase(1916, DOUBLE, TIME, "cast(42 as double)",
                        "42", "Cannot assign to target field '.+' of type TIME from source field '.+' of type DOUBLE"),
                TestParams.failingCase(1917, DOUBLE, DATE, "cast(42 as double)",
                        "42", "Cannot assign to target field '.+' of type DATE from source field '.+' of type DOUBLE"),
                TestParams.failingCase(1918, DOUBLE, TIMESTAMP, "cast(42 as double)",
                        "42", "Cannot assign to target field '.+' of type TIMESTAMP from source field '.+' of type DOUBLE"),
                TestParams.failingCase(1919, DOUBLE, TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, "cast(42 as double)",
                        "42", "Cannot assign to target field '.+' of type TIMESTAMP WITH TIME ZONE from source field '.+' of type DOUBLE"),
                TestParams.passingCase(1920, DOUBLE, OBJECT, "cast(42 as double)", "42", 42d),

                // TIME
                TestParams.failingCase(2001, TIME, VARCHAR, "cast('01:42:00' as time)", "01:42:00",
                        "Cannot assign to target field '.+' of type VARCHAR from source field '.+' of type TIME"),
                TestParams.failingCase(2002, TIME, BOOLEAN, "cast('01:42:00' as time)",
                        "01:42:00", "Cannot assign to target field '.+' of type BOOLEAN from source field '.+' of type TIME"),
                TestParams.failingCase(2003, TIME, TINYINT, "cast('1:42:00' as time)",
                        "01:42:00", "Cannot assign to target field '.+' of type TINYINT from source field '.+' of type TIME"),
                TestParams.failingCase(2004, TIME, SMALLINT, "cast('1:42:00' as time)",
                        "01:42:00", "Cannot assign to target field '.+' of type SMALLINT from source field '.+' of type TIME"),
                TestParams.failingCase(2005, TIME, INT, "cast('1:42:00' as time)",
                        "01:42:00", "Cannot assign to target field '.+' of type INTEGER from source field '.+' of type TIME"),
                TestParams.failingCase(2006, TIME, BIGINT, "cast('1:42:00' as time)",
                        "01:42:00", "Cannot assign to target field '.+' of type BIGINT from source field '.+' of type TIME"),
                TestParams.failingCase(2007, TIME, DECIMAL, "cast('1:42:00' as time)",
                        "01:42:00", "Cannot assign to target field '.+' of type DECIMAL\\(38, 38\\) from source field '.+' of type TIME"),
                TestParams.failingCase(2008, TIME, REAL, "cast('1:42:00' as time)",
                        "01:42:00", "Cannot assign to target field '.+' of type REAL from source field '.+' of type TIME"),
                TestParams.failingCase(2009, TIME, DOUBLE, "cast('1:42:00' as time)",
                        "01:42:00", "Cannot assign to target field '.+' of type DOUBLE from source field '.+' of type TIME"),
                TestParams.passingCase(2010, TIME, TIME, "cast('01:42:00' as time)", "01:42:00", LocalTime.of(1, 42)),
                TestParams.failingCase(2011, TIME, DATE, "cast('01:42:00' as time)",
                        "01:42:00", "Cannot assign to target field '.+' of type DATE from source field '.+' of type TIME"),
                // this variant can fail around midnight
                TestParams.passingCase(2012, TIME, TIMESTAMP, "cast('01:42:00' as time)", "01:42:00",
                        LocalDateTime.of(LocalDate.now(), LocalTime.of(1, 42))),
                // this variant can fail around midnight
                TestParams.passingCase(2013, TIME, TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, "cast('01:42:00' as time)", "01:42:00",
                        ZonedDateTime.of(
                                LocalDateTime.of(LocalDate.now(), LocalTime.of(1, 42)),
                                DEFAULT_ZONE).toOffsetDateTime()),
                TestParams.passingCase(2014, TIME, OBJECT, "cast('01:42:00' as time)", "01:42:00", LocalTime.of(1, 42)),

                // DATE
                TestParams.failingCase(2101, DATE, VARCHAR, "cast('2020-12-30' as date)", "2020-12-30",
                        "Cannot assign to target field '.+' of type VARCHAR from source field '.+' of type DATE"),
                TestParams.failingCase(2102, DATE, BOOLEAN, "cast('2020-12-30' as date)",
                        "2020-12-30", "Cannot assign to target field '.+' of type BOOLEAN from source field '.+' of type DATE"),
                TestParams.failingCase(2103, DATE, TINYINT, "cast('2020-12-30' as date)",
                        "2020-12-30", "Cannot assign to target field '.+' of type TINYINT from source field '.+' of type DATE"),
                TestParams.failingCase(2104, DATE, SMALLINT, "cast('2020-12-30' as date)",
                        "2020-12-30", "Cannot assign to target field '.+' of type SMALLINT from source field '.+' of type DATE"),
                TestParams.failingCase(2105, DATE, INT, "cast('2020-12-30' as date)",
                        "2020-12-30", "Cannot assign to target field '.+' of type INTEGER from source field '.+' of type DATE"),
                TestParams.failingCase(2106, DATE, BIGINT, "cast('2020-12-30' as date)",
                        "2020-12-30", "Cannot assign to target field '.+' of type BIGINT from source field '.+' of type DATE"),
                TestParams.failingCase(2107, DATE, DECIMAL, "cast('2020-12-30' as date)",
                        "2020-12-30", "Cannot assign to target field '.+' of type DECIMAL\\(38, 38\\) from source field '.+' of type DATE"),
                TestParams.failingCase(2108, DATE, REAL, "cast('2020-12-30' as date)",
                        "2020-12-30", "Cannot assign to target field '.+' of type REAL from source field '.+' of type DATE"),
                TestParams.failingCase(2109, DATE, DOUBLE, "cast('2020-12-30' as date)",
                        "2020-12-30", "Cannot assign to target field '.+' of type DOUBLE from source field '.+' of type DATE"),
                TestParams.failingCase(2110, DATE, TIME, "cast('2020-12-30' as date)",
                        "2020-12-30", "Cannot assign to target field '.+' of type TIME from source field '.+' of type DATE"),
                TestParams.passingCase(2111, DATE, DATE, "cast('2020-12-30' as date)", "2020-12-30",
                        LocalDate.of(2020, 12, 30)),
                TestParams.passingCase(2112, DATE, TIMESTAMP, "cast('2020-12-30' as date)", "2020-12-30",
                        LocalDateTime.of(2020, 12, 30, 0, 0)),
                TestParams.passingCase(2113, DATE, TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, "cast('2020-12-30' as date)", "2020-12-30",
                        ZonedDateTime.of(2020, 12, 30, 0, 0, 0, 0, DEFAULT_ZONE).toOffsetDateTime()),
                TestParams.passingCase(2114, DATE, OBJECT, "cast('2020-12-30' as date)", "2020-12-30",
                        LocalDate.of(2020, 12, 30)),

                // TIMESTAMP
                TestParams.failingCase(2201, TIMESTAMP, VARCHAR, "cast('2020-12-30T01:42:00' as timestamp)", "2020-12-30T01:42:00",
                        "Cannot assign to target field '.+' of type VARCHAR from source field '.+' of type TIMESTAMP"),
                TestParams.failingCase(2202, TIMESTAMP, BOOLEAN, "cast('2020-12-30T01:42:00' as timestamp)",
                        "2020-12-30T01:42:00", "Cannot assign to target field '.+' of type BOOLEAN from source field '.+' of type TIMESTAMP"),
                TestParams.failingCase(2203, TIMESTAMP, TINYINT, "cast('2020-12-30T01:42:00' as timestamp)",
                        "2020-12-30T01:42:00", "Cannot assign to target field '.+' of type TINYINT from source field '.+' of type TIMESTAMP"),
                TestParams.failingCase(2204, TIMESTAMP, SMALLINT, "cast('2020-12-30T01:42:00' as timestamp)",
                        "2020-12-30T01:42:00", "Cannot assign to target field '.+' of type SMALLINT from source field '.+' of type TIMESTAMP"),
                TestParams.failingCase(2205, TIMESTAMP, INT, "cast('2020-12-30T01:42:00' as timestamp)",
                        "2020-12-30T01:42:00", "Cannot assign to target field '.+' of type INTEGER from source field '.+' of type TIMESTAMP"),
                TestParams.failingCase(2206, TIMESTAMP, BIGINT, "cast('2020-12-30T01:42:00' as timestamp)",
                        "2020-12-30T01:42:00", "Cannot assign to target field '.+' of type BIGINT from source field '.+' of type TIMESTAMP"),
                TestParams.failingCase(2207, TIMESTAMP, DECIMAL, "cast('2020-12-30T01:42:00' as timestamp)",
                        "2020-12-30T01:42:00", "Cannot assign to target field '.+' of type DECIMAL\\(38, 38\\) from source field '.+' of type TIMESTAMP"),
                TestParams.failingCase(2208, TIMESTAMP, REAL, "cast('2020-12-30T01:42:00' as timestamp)",
                        "2020-12-30T01:42:00", "Cannot assign to target field '.+' of type REAL from source field '.+' of type TIMESTAMP"),
                TestParams.failingCase(2209, TIMESTAMP, DOUBLE, "cast('2020-12-30T01:42:00' as timestamp)",
                        "2020-12-30T01:42:00", "Cannot assign to target field '.+' of type DOUBLE from source field '.+' of type TIMESTAMP"),
                TestParams.passingCase(2210, TIMESTAMP, TIME, "cast('2020-12-30T01:42:00' as timestamp)", "2020-12-30T01:42:00",
                        LocalTime.of(1, 42)),
                TestParams.passingCase(2211, TIMESTAMP, DATE, "cast('2020-12-30T01:42:00' as timestamp)", "2020-12-30T01:42:00",
                        LocalDate.of(2020, 12, 30)),
                TestParams.passingCase(2212, TIMESTAMP, TIMESTAMP, "cast('2020-12-30T01:42:00' as timestamp)", "2020-12-30T01:42:00",
                        LocalDateTime.of(2020, 12, 30, 1, 42)),
                TestParams.passingCase(2213, TIMESTAMP, TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, "cast('2020-12-30T01:42:00' as timestamp)",
                        "2020-12-30T01:42:00", ZonedDateTime.of(2020, 12, 30, 1, 42, 0, 0, DEFAULT_ZONE).toOffsetDateTime()),
                TestParams.passingCase(2214, TIMESTAMP, OBJECT, "cast('2020-12-30T01:42:00' as timestamp)", "2020-12-30T01:42:00",
                        LocalDateTime.of(2020, 12, 30, 1, 42)),

                // TIMESTAMP WITH TIME ZONE
                TestParams.failingCase(2301, TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, VARCHAR, "cast('2020-12-30T01:42:00-05:00' as timestamp with time zone)",
                        "2020-12-30T01:42:00-05:00", "Cannot assign to target field '.+' of type VARCHAR from source field '.+' of type TIMESTAMP WITH TIME ZONE"),
                TestParams.failingCase(2302, TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, BOOLEAN, "cast('2020-12-30T01:42:00-05:00' as timestamp with time zone)",
                        "2020-12-30T01:42:00-05:00", "Cannot assign to target field '.+' of type BOOLEAN from source field '.+' of type TIMESTAMP WITH TIME ZONE"),
                TestParams.failingCase(2303, TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, TINYINT, "cast('2020-12-30T01:42:00-05:00' as timestamp with time zone)",
                        "2020-12-30T01:42:00-05:00", "Cannot assign to target field '.+' of type TINYINT from source field '.+' of type TIMESTAMP WITH TIME ZONE"),
                TestParams.failingCase(2304, TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, SMALLINT, "cast('2020-12-30T01:42:00-05:00' as timestamp with time zone)",
                        "2020-12-30T01:42:00-05:00", "Cannot assign to target field '.+' of type SMALLINT from source field '.+' of type TIMESTAMP WITH TIME ZONE"),
                TestParams.failingCase(2305, TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, INT, "cast('2020-12-30T01:42:00-05:00' as timestamp with time zone)",
                        "2020-12-30T01:42:00-05:00", "Cannot assign to target field '.+' of type INTEGER from source field '.+' of type TIMESTAMP WITH TIME ZONE"),
                TestParams.failingCase(2306, TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, BIGINT, "cast('2020-12-30T01:42:00-05:00' as timestamp with time zone)",
                        "2020-12-30T01:42:00-05:00", "Cannot assign to target field '.+' of type BIGINT from source field '.+' of type TIMESTAMP WITH TIME ZONE"),
                TestParams.failingCase(2307, TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, DECIMAL, "cast('2020-12-30T01:42:00-05:00' as timestamp with time zone)",
                        "2020-12-30T01:42:00-05:00", "Cannot assign to target field '.+' of type DECIMAL\\(38, 38\\) from source field '.+' of type TIMESTAMP WITH TIME ZONE"),
                TestParams.failingCase(2308, TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, REAL, "cast('2020-12-30T01:42:00-05:00' as timestamp with time zone)",
                        "2020-12-30T01:42:00-05:00", "Cannot assign to target field '.+' of type REAL from source field '.+' of type TIMESTAMP WITH TIME ZONE"),
                TestParams.failingCase(2309, TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, DOUBLE, "cast('2020-12-30T01:42:00-05:00' as timestamp with time zone)",
                        "2020-12-30T01:42:00-05:00", "Cannot assign to target field '.+' of type DOUBLE from source field '.+' of type TIMESTAMP WITH TIME ZONE"),
                TestParams.passingCase(2310, TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, TIME, "cast('2020-12-30T01:42:00-05:00' as timestamp with time zone)",
                        "2020-12-30T01:42:00-05:00", LocalTime.of(1, 42)),
                TestParams.passingCase(2311, TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, DATE, "cast('2020-12-30T01:42:00-05:00' as timestamp with time zone)",
                        "2020-12-30T01:42:00-05:00", LocalDate.of(2020, 12, 30)),
                TestParams.passingCase(2312, TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, TIMESTAMP, "cast('2020-12-30T01:42:00-05:00' as timestamp with time zone)",
                        "2020-12-30T01:42:00-05:00", LocalDateTime.of(2020, 12, 30, 1, 42)),
                TestParams.passingCase(2313, TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, "cast('2020-12-30T01:42:00-05:00' as timestamp with time zone)",
                        "2020-12-30T01:42:00-05:00", OffsetDateTime.of(2020, 12, 30, 1, 42, 0, 0, ZoneOffset.ofHours(-5))),
                TestParams.passingCase(2314, TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, OBJECT, "cast('2020-12-30T01:42:00-05:00' as timestamp with time zone)",
                        "2020-12-30T01:42:00-05:00", OffsetDateTime.of(2020, 12, 30, 1, 42, 0, 0, ZoneOffset.ofHours(-5))),

                // OBJECT
                TestParams.failingCase(2401, OBJECT, VARCHAR, "cast('foo' as object)", null,
                        "Cannot assign to target field '.+' of type VARCHAR from source field 'EXPR\\$0' of type OBJECT"),
                TestParams.failingCase(2402, OBJECT, BOOLEAN, "cast(true as object)", null,
                        "Cannot assign to target field '.+' of type BOOLEAN from source field 'EXPR\\$0' of type OBJECT"),
                TestParams.failingCase(2403, OBJECT, TINYINT, "cast(42 as object)", null,
                        "Cannot assign to target field '.+' of type TINYINT from source field 'EXPR\\$0' of type OBJECT"),
                TestParams.failingCase(2404, OBJECT, SMALLINT, "cast(420 as object)", null,
                        "Cannot assign to target field '.+' of type SMALLINT from source field 'EXPR\\$0' of type OBJECT"),
                TestParams.failingCase(2405, OBJECT, INT, "cast(420000 as object)", null,
                        "Cannot assign to target field '.+' of type INTEGER from source field 'EXPR\\$0' of type OBJECT"),
                TestParams.failingCase(2406, OBJECT, BIGINT, "cast(4200000000 as object)", null,
                        "Cannot assign to target field '.+' of type BIGINT from source field 'EXPR\\$0' of type OBJECT"),
                TestParams.failingCase(2407, OBJECT, DECIMAL, "cast(cast(1.5 as decimal) as object)", null,
                        "Cannot assign to target field '.+' of type DECIMAL\\(38, 38\\) from source field 'EXPR\\$0' of type OBJECT"),
                TestParams.failingCase(2408, OBJECT, REAL, "cast(cast(1.5 as real) as object)", null,
                        "Cannot assign to target field '.+' of type REAL from source field 'EXPR\\$0' of type OBJECT"),
                TestParams.failingCase(2409, OBJECT, DOUBLE, "cast(cast(1.5 as double) as object)", null,
                        "Cannot assign to target field '.+' of type DOUBLE from source field 'EXPR\\$0' of type OBJECT"),
                TestParams.failingCase(2410, OBJECT, TIME, "cast(cast('01:42:00' as time) as object)", null,
                        "Cannot assign to target field '.+' of type TIME from source field 'EXPR\\$0' of type OBJECT"),
                TestParams.failingCase(2411, OBJECT, DATE, "cast(cast('2020-12-30' as date) as object)", null,
                        "Cannot assign to target field '.+' of type DATE from source field 'EXPR\\$0' of type OBJECT"),
                TestParams.failingCase(2412, OBJECT, TIMESTAMP, "cast(cast('2020-12-30T01:42:00' as timestamp) as object)", null,
                        "Cannot assign to target field '.+' of type TIMESTAMP from source field 'EXPR\\$0' of type OBJECT"),
                TestParams.failingCase(2413, OBJECT, TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME,
                        "cast(cast('2020-12-30T01:42:00-05:00' as timestamp with time zone) as object)", null,
                        "Cannot assign to target field '.+' of type TIMESTAMP WITH TIME ZONE from source field 'EXPR\\$0' of type OBJECT"),
                TestParams.passingCase(2414, OBJECT, OBJECT, "cast('foo' as object)",
                        "foo", "foo"),
        };
    }

    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
    }

    @Test
    public void test_update_literals() throws Exception {
        // TODO remove this once we support the TIMESTAMP and TIMESTAMP_WITH_TIME_ZONE literals
        assumeFalse(testParams.targetType == TIMESTAMP || testParams.targetType == TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME);

        // these fail due to a calcite issue that converts temporal literals casted to OBJECT to INT
        // or BIGINT casted to OBJECT
        assumeFalse(testParams.sourceType == OBJECT && testParams.targetType.getTypeFamily().isTemporal());

        String targetClassName = ExpressionValue.classForType(testParams.targetType.getTypeFamily());
        sqlService.execute("CREATE MAPPING m type IMap " +
                "OPTIONS (" +
                "'keyFormat'='int', " +
                "'valueFormat'='java', " +
                "'valueJavaClass'='" + targetClassName +
                "')"
        );
        instance().getMap("m").put(0, ExpressionValue.create(targetClassName));

        try {
            sqlService.execute("UPDATE m SET field1 = " + testParams.valueLiteral);
            if (testParams.expectedFailureRegex != null) {
                fail("Expected to fail with \"" + testParams.expectedFailureRegex + "\", but no exception was thrown");
            }
            assertEquals(testParams.targetValue, extractValue("m", "field1"));
        } catch (Exception e) {
            if (testParams.expectedFailureRegex == null) {
                throw e;
            }
            if (!testParams.failureMatches(e)) {
                throw new AssertionError("\n'" + e.getMessage() + "'\ndidn't contain the regexp \n'"
                        + testParams.expectedFailureRegex + "'", e);
            }
        }
    }

    @Test
    public void test_update_columns() throws NoSuchFieldException, IllegalAccessException {
        assumeFalse(testParams.sourceType == NULL);

        String sourceField = toFieldName(testParams.sourceType);
        String targetField = toFieldName(testParams.targetType);

        String targetClassName = AllValues.class.getName();
        sqlService.execute("CREATE MAPPING m TYPE IMap " +
                "OPTIONS(" +
                "'keyFormat'='int', " +
                "'valueFormat'='java', " +
                "'valueJavaClass'='" + targetClassName +
                "')"
        );
        AllValues allValues = new AllValues();
        AllValues.class.getField(sourceField).set(allValues, testParams.sourceType.convert(testParams.valueTestSource));
        instance().getMap("m").put(0, allValues);

        try {
            sqlService.execute("UPDATE m SET " + targetField + " = " + sourceField);
            if (testParams.expectedFailureNonLiteralRegex != null) {
                fail("Expected to fail with \"" + testParams.expectedFailureNonLiteralRegex + "\", but no exception was thrown");
            }
            if (testParams.expectedFailureRegex != null) {
                fail("Expected to fail with \"" + testParams.expectedFailureRegex + "\", but no exception was thrown");
            }
            assertEquals(testParams.targetValue, extractValue("m", targetField));
        } catch (Exception e) {
            if (testParams.expectedFailureRegex == null && testParams.expectedFailureNonLiteralRegex == null) {
                throw new AssertionError("The query failed unexpectedly: " + e, e);
            }
            if (testParams.expectedFailureNonLiteralRegex != null) {
                if (!testParams.nonLiteralFailureMatches(e)) {
                    throw new AssertionError("\n'" + e.getMessage() + "'\ndidn't contain \n'"
                            + testParams.expectedFailureNonLiteralRegex + "'", e);
                }
            } else if (!testParams.failureMatches(e)) {
                throw new AssertionError("\n'" + e.getMessage() + "'\ndidn't contain the regexp \n'"
                        + testParams.expectedFailureRegex + "'", e);
            }
        }
    }

    private static String toFieldName(QueryDataType type) {
        return type.getTypeFamily().toString().replace(' ', '_').toLowerCase() + "Field";
    }

    private static Object extractValue(String mapName, String fieldName) throws NoSuchFieldException, IllegalAccessException {
        Object valueWrapper = instance().getMap(mapName).get(0);
        return valueWrapper.getClass().getField(fieldName).get(valueWrapper);
    }

    @SuppressWarnings("unused")
    public static final class AllValues implements Serializable {
        public Boolean booleanField;
        public Byte tinyintField;
        public Short smallintField;
        public Integer integerField;
        public Long bigintField;
        public BigDecimal decimalField;
        public Float realField;
        public Double doubleField;
        public String varcharField;
        public LocalTime timeField;
        public LocalDate dateField;
        public LocalDateTime timestampField;
        public OffsetDateTime timestamp_with_time_zoneField;
        public Object nullField;
        public Object objectField;
    }

    private static final class TestParams {

        private final int testId;
        private final QueryDataType sourceType;
        private final QueryDataType targetType;
        private final String valueLiteral;
        private final String valueTestSource;
        private final Object targetValue;
        private final Pattern expectedFailureRegex;
        private Pattern expectedFailureNonLiteralRegex;

        private TestParams(
                int testId,
                QueryDataType sourceType,
                QueryDataType targetType,
                String valueLiteral,
                String valueTestSource,
                Object targetValue,
                String expectedFailureRegex
        ) {
            this.testId = testId;
            this.sourceType = sourceType;
            this.targetType = targetType;
            this.valueLiteral = valueLiteral;
            this.valueTestSource = valueTestSource;
            this.targetValue = targetValue;
            this.expectedFailureRegex = expectedFailureRegex != null ? Pattern.compile(expectedFailureRegex) : null;
        }

        private static TestParams passingCase(
                int testId,
                QueryDataType sourceType,
                QueryDataType targetType,
                String valueLiteral,
                String valueTestSource,
                Object targetValue
        ) {
            return new TestParams(testId, sourceType, targetType, valueLiteral, valueTestSource, targetValue, null);
        }

        private static TestParams failingCase(
                int testId,
                QueryDataType sourceType,
                QueryDataType targetType,
                String valueLiteral,
                String valueTestSource,
                String errorMessage
        ) {
            return new TestParams(testId, sourceType, targetType, valueLiteral, valueTestSource, null, errorMessage);
        }

        @SuppressWarnings("BooleanMethodIsAlwaysInverted")
        private boolean failureMatches(Exception e) {
            return expectedFailureRegex.matcher(e.getMessage()).find();
        }

        /**
         * This test case is expected to fail only if the source is a column, not a
         * literal (the {@link #test_update_columns()} method). Useful for coercion
         * which is allowed for literals, but not for expressions or columns
         * otherwise. For example, you can assign a VARCHAR literal to a DATE
         * column, but you can't assign VARCHAR column or expression to a date
         * column.
         */
        private TestParams setExpectedFailureNonLiteralRegex(String expectedFailureNonLiteralRegex) {
            this.expectedFailureNonLiteralRegex = Pattern.compile(expectedFailureNonLiteralRegex);
            return this;
        }

        @SuppressWarnings("BooleanMethodIsAlwaysInverted")
        private boolean nonLiteralFailureMatches(Exception e) {
            return expectedFailureNonLiteralRegex.matcher(e.getMessage()).find();
        }

        @Override
        public String toString() {
            return "TestParams{" +
                    "id=" + testId +
                    ", sourceType=" + sourceType +
                    ", targetType=" + targetType +
                    ", valueLiteral=" + valueLiteral +
                    ", valueTestSource=" + valueTestSource +
                    ", targetValue=" + targetValue +
                    ", expectedFailureRegex=" + expectedFailureRegex +
                    '}';
        }
    }
}
