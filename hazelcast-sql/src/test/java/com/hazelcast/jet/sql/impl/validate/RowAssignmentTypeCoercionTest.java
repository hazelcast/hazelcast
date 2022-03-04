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

package com.hazelcast.jet.sql.impl.validate;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.test.TestBatchSqlConnector;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionBiValue;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionValue;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
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
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.BIGINT;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.BOOLEAN;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.DATE;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.DECIMAL;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.DOUBLE;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.INTEGER;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.JSON;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.NULL;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.OBJECT;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.REAL;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.SMALLINT;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.TIME;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.TIMESTAMP;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.TIMESTAMP_WITH_TIME_ZONE;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.TINYINT;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.VARCHAR;
import static com.hazelcast.sql.impl.type.converter.AbstractTemporalConverter.DEFAULT_ZONE;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.Assume.assumeFalse;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
public class RowAssignmentTypeCoercionTest extends SqlTestSupport {

    private static final LocalDate TODAY = LocalDate.now();
    private static final LocalDate TOMORROW = TODAY.plusDays(1);

    @Parameter
    public TestParams testParams;

    private final SqlService sqlService = instance().getSql();

    @SuppressWarnings({"checkstyle:LineLength", "checkstyle:MethodLength"})
    @Parameters(name = "{0}")
    public static Object[] parameters() {
        return new Object[]{
                // NULL
                TestParams.passingCase(1001, NULL, VARCHAR, "null", null, (Object) null),
                TestParams.passingCase(1002, NULL, BOOLEAN, "null", null, (Object) null),
                TestParams.passingCase(1003, NULL, TINYINT, "null", null, (Object) null),
                TestParams.passingCase(1004, NULL, SMALLINT, "null", null, (Object) null),
                TestParams.passingCase(1005, NULL, INTEGER, "null", null, (Object) null),
                TestParams.passingCase(1006, NULL, BIGINT, "null", null, (Object) null),
                TestParams.passingCase(1007, NULL, DECIMAL, "null", null, (Object) null),
                TestParams.passingCase(1008, NULL, REAL, "null", null, (Object) null),
                TestParams.passingCase(1009, NULL, DOUBLE, "null", null, (Object) null),
                TestParams.passingCase(1010, NULL, TIME, "null", null, (Object) null),
                TestParams.passingCase(1011, NULL, DATE, "null", null, (Object) null),
                TestParams.passingCase(1012, NULL, TIMESTAMP, "null", null, (Object) null),
                TestParams.passingCase(1013, NULL, TIMESTAMP_WITH_TIME_ZONE, null, null, (Object) null),
                TestParams.passingCase(1014, NULL, OBJECT, "null", null, (Object) null),

                // VARCHAR
                TestParams.passingCase(1101, VARCHAR, VARCHAR, "'foo'", "foo", "foo"),
                TestParams.failingCase(1102, VARCHAR, BOOLEAN, "'true'", "true")
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type BOOLEAN from source field '.+' of type VARCHAR")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type BOOLEAN from source field '.+' of type VARCHAR")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of BOOLEAN type, but VARCHAR was found"),
                TestParams.failingCase(1103, VARCHAR, TINYINT, "'42'", "42")
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type TINYINT from source field '.+' of type VARCHAR")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TINYINT from source field '.+' of type VARCHAR")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TINYINT type, but VARCHAR was found"),
                TestParams.failingCase(1104, VARCHAR, TINYINT, "'420'", "420")
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type TINYINT from source field '.+' of type VARCHAR")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TINYINT from source field '.+' of type VARCHAR")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TINYINT type, but VARCHAR was found"),
                TestParams.failingCase(1105, VARCHAR, TINYINT, "'foo'", "foo")
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type TINYINT from source field '.+' of type VARCHAR")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TINYINT from source field '.+' of type VARCHAR")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TINYINT type, but VARCHAR was found"),
                TestParams.failingCase(1106, VARCHAR, SMALLINT, "'42'", "42")
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type SMALLINT from source field '.+' of type VARCHAR")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type SMALLINT from source field '.+' of type VARCHAR")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of SMALLINT type, but VARCHAR was found"),
                TestParams.failingCase(1107, VARCHAR, SMALLINT, "'42000'", "42000")
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type SMALLINT from source field '.+' of type VARCHAR")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type SMALLINT from source field '.+' of type VARCHAR")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of SMALLINT type, but VARCHAR was found"),
                TestParams.failingCase(1108, VARCHAR, SMALLINT, "'foo'", "foo")
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type SMALLINT from source field '.+' of type VARCHAR")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type SMALLINT from source field '.+' of type VARCHAR")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of SMALLINT type, but VARCHAR was found"),
                TestParams.failingCase(1109, VARCHAR, INTEGER, "'42'", "42")
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type INTEGER from source field '.+' of type VARCHAR")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type INTEGER from source field '.+' of type VARCHAR")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of INTEGER type, but VARCHAR was found"),
                TestParams.failingCase(1110, VARCHAR, INTEGER, "'4200000000'", "4200000000")
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type INTEGER from source field '.+' of type VARCHAR")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type INTEGER from source field '.+' of type VARCHAR")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of INTEGER type, but VARCHAR was found"),
                TestParams.failingCase(1111, VARCHAR, INTEGER, "'foo'", "foo")
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type INTEGER from source field '.+' of type VARCHAR")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type INTEGER from source field '.+' of type VARCHAR")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of INTEGER type, but VARCHAR was found"),
                TestParams.failingCase(1112, VARCHAR, BIGINT, "'42'", "42")
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type BIGINT from source field '.+' of type VARCHAR")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type BIGINT from source field '.+' of type VARCHAR")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of BIGINT type, but VARCHAR was found"),
                TestParams.failingCase(1113, VARCHAR, BIGINT, "'9223372036854775808000'", "9223372036854775808000")
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type BIGINT from source field '.+' of type VARCHAR")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type BIGINT from source field '.+' of type VARCHAR")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of BIGINT type, but VARCHAR was found"),
                TestParams.failingCase(1114, VARCHAR, BIGINT, "'foo'", "foo")
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type BIGINT from source field '.+' of type VARCHAR")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type BIGINT from source field '.+' of type VARCHAR")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of BIGINT type, but VARCHAR was found"),
                TestParams.failingCase(1115, VARCHAR, DECIMAL, "'1.5'", "1.5")
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type DECIMAL\\(76, 38\\) from source field '.+' of type VARCHAR")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type DECIMAL\\(76, 38\\) from source field '.+' of type VARCHAR")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of DECIMAL type, but VARCHAR was found"),
                TestParams.failingCase(1116, VARCHAR, DECIMAL, "'foo'", "foo")
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type DECIMAL\\(76, 38\\) from source field '.+' of type VARCHAR")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type DECIMAL\\(76, 38\\) from source field '.+' of type VARCHAR")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of DECIMAL type, but VARCHAR was found"),
                TestParams.failingCase(1117, VARCHAR, REAL, "'1.5'", "1.5")
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type REAL from source field '.+' of type VARCHAR")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type REAL from source field '.+' of type VARCHAR")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of REAL type, but VARCHAR was found"),
                TestParams.failingCase(1118, VARCHAR, REAL, "'foo'", "foo")
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type REAL from source field '.+' of type VARCHAR")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type REAL from source field '.+' of type VARCHAR")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of REAL type, but VARCHAR was found"),
                TestParams.failingCase(1119, VARCHAR, DOUBLE, "'1.5'", "1.5")
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type DOUBLE from source field '.+' of type VARCHAR")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type DOUBLE from source field '.+' of type VARCHAR")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of DOUBLE type, but VARCHAR was found"),
                TestParams.failingCase(1120, VARCHAR, DOUBLE, "'foo'", "foo")
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type DOUBLE from source field '.+' of type VARCHAR")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type DOUBLE from source field '.+' of type VARCHAR")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of DOUBLE type, but VARCHAR was found"),
                TestParams.passingCase(1121, VARCHAR, TIME, "'01:42:01'", "01:42:01", LocalTime.of(1, 42, 1))
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TIME from source field '.+' of type VARCHAR")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TIME type, but VARCHAR was found"),
                TestParams.failingCase(1122, VARCHAR, TIME, "'foo'", "foo")
                        .withExpectedLiteralFailureRegex("Cannot parse VARCHAR value to TIME")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TIME from source field '.+' of type VARCHAR")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TIME type, but VARCHAR was found"),
                TestParams.passingCase(1123, VARCHAR, DATE, "'2020-12-30'", "2020-12-30", LocalDate.of(2020, 12, 30))
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type DATE from source field '.+' of type VARCHAR")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of DATE type, but VARCHAR was found"),
                TestParams.failingCase(1124, VARCHAR, DATE, "'foo'", "foo")
                        .withExpectedLiteralFailureRegex("Cannot parse VARCHAR value to DATE")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of DATE type, but VARCHAR was found")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type DATE from source field '.+' of type VARCHAR"),
                TestParams.passingCase(1125, VARCHAR, TIMESTAMP, "'2020-12-30T01:42:00'", "2020-12-30T01:42:00", LocalDateTime.of(2020, 12, 30, 1, 42))
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TIMESTAMP from source field '.+' of type VARCHAR")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TIMESTAMP type, but VARCHAR was found"),
                TestParams.failingCase(1126, VARCHAR, TIMESTAMP, "'foo'", "foo")
                        .withExpectedLiteralFailureRegex("Cannot parse VARCHAR value to TIMESTAMP")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TIMESTAMP from source field '.+' of type VARCHAR")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TIMESTAMP type, but VARCHAR was found"),
                TestParams.passingCase(1127, VARCHAR, TIMESTAMP_WITH_TIME_ZONE, "'2020-12-30T01:42:00-05:00'", "2020-12-30T01:42:00-05:00", OffsetDateTime.of(2020, 12, 30, 1, 42, 0, 0, ZoneOffset.ofHours(-5)))
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TIMESTAMP WITH TIME ZONE from source field '.+' of type VARCHAR")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TIMESTAMP WITH TIME ZONE type, but VARCHAR was found"),
                TestParams.failingCase(1128, VARCHAR, TIMESTAMP_WITH_TIME_ZONE, "'foo'", "foo")
                        .withExpectedLiteralFailureRegex("Cannot parse VARCHAR value to TIMESTAMP WITH TIME ZONE")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TIMESTAMP WITH TIME ZONE from source field '.+' of type VARCHAR")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TIMESTAMP WITH TIME ZONE type, but VARCHAR was found"),
                TestParams.passingCase(1129, VARCHAR, OBJECT, "'foo'", "foo", "foo"),
                TestParams.passingCase(1130, VARCHAR, JSON, "'foo'", "foo", new HazelcastJsonValue("foo")),

                // BOOLEAN
                TestParams.failingCase(1201, BOOLEAN, VARCHAR, "true", true)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type VARCHAR from source field '.+' of type BOOLEAN")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type VARCHAR from source field '.+' of type BOOLEAN")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of VARCHAR type, but BOOLEAN was found"),
                TestParams.passingCase(1202, BOOLEAN, BOOLEAN, "true", true, true),
                TestParams.failingCase(1203, BOOLEAN, TINYINT, "true", true)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type TINYINT from source field '(EXPR\\$\\d|v)' of type BOOLEAN")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TINYINT from source field '(EXPR\\$\\d|v)' of type BOOLEAN")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TINYINT type, but BOOLEAN was found"),
                TestParams.failingCase(1204, BOOLEAN, SMALLINT, "true", true)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type SMALLINT from source field '(EXPR\\$\\d|v)' of type BOOLEAN")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type SMALLINT from source field '(EXPR\\$\\d|v)' of type BOOLEAN")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of SMALLINT type, but BOOLEAN was found"),
                TestParams.failingCase(1205, BOOLEAN, INTEGER, "true", true)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type INTEGER from source field '(EXPR\\$\\d|v)' of type BOOLEAN")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type INTEGER from source field '(EXPR\\$\\d|v)' of type BOOLEAN")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of INTEGER type, but BOOLEAN was found"),
                TestParams.failingCase(1206, BOOLEAN, BIGINT, "true", true)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type BIGINT from source field '(EXPR\\$\\d|v)' of type BOOLEAN")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type BIGINT from source field '(EXPR\\$\\d|v)' of type BOOLEAN")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of BIGINT type, but BOOLEAN was found"),
                TestParams.failingCase(1207, BOOLEAN, DECIMAL, "true", true)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type DECIMAL\\(76, 38\\) from source field '(EXPR\\$\\d|v)' of type BOOLEAN")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type DECIMAL\\(76, 38\\) from source field '(EXPR\\$\\d|v)' of type BOOLEAN")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of DECIMAL type, but BOOLEAN was found"),
                TestParams.failingCase(1208, BOOLEAN, REAL, "true", true)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type REAL from source field '(EXPR\\$\\d|v)' of type BOOLEAN")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type REAL from source field '(EXPR\\$\\d|v)' of type BOOLEAN")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of REAL type, but BOOLEAN was found"),
                TestParams.failingCase(1209, BOOLEAN, DOUBLE, "true", true)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type DOUBLE from source field '(EXPR\\$\\d|v)' of type BOOLEAN")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type DOUBLE from source field '(EXPR\\$\\d|v)' of type BOOLEAN")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of DOUBLE type, but BOOLEAN was found"),
                TestParams.failingCase(1210, BOOLEAN, TIME, "true", true)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type TIME from source field '.+' of type BOOLEAN")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TIME from source field '.+' of type BOOLEAN")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TIME type, but BOOLEAN was found"),
                TestParams.failingCase(1211, BOOLEAN, DATE, "true", true)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type DATE from source field '.+' of type BOOLEAN")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type DATE from source field '.+' of type BOOLEAN")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of DATE type, but BOOLEAN was found"),
                TestParams.failingCase(1212, BOOLEAN, TIMESTAMP, "true", true)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type TIMESTAMP from source field '.+' of type BOOLEAN")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TIMESTAMP from source field '.+' of type BOOLEAN")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TIMESTAMP type, but BOOLEAN was found"),
                TestParams.failingCase(1213, BOOLEAN, TIMESTAMP_WITH_TIME_ZONE, "true", true)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type TIMESTAMP WITH TIME ZONE from source field '.+' of type BOOLEAN")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TIMESTAMP WITH TIME ZONE from source field '.+' of type BOOLEAN")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TIMESTAMP WITH TIME ZONE type, but BOOLEAN was found"),
                TestParams.passingCase(1214, BOOLEAN, OBJECT, "true", true, true),
                TestParams.failingCase(1215, BOOLEAN, JSON, "true", true)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type JSON from source field '(EXPR\\$\\d|v)' of type BOOLEAN")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type JSON from source field '(EXPR\\$\\d|v)' of type BOOLEAN")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of JSON type, but BOOLEAN was found"),

                // TINYINT
                TestParams.failingCase(1301, TINYINT, VARCHAR, "cast(42 as tinyint)", (byte) 42)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type VARCHAR from source field '.+' of type TINYINT")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type VARCHAR from source field '.+' of type TINYINT")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of VARCHAR type, but TINYINT was found"),
                TestParams.failingCase(1302, TINYINT, BOOLEAN, "cast(42 as tinyint)", (byte) 42)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type BOOLEAN from source field '.+' of type TINYINT")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type BOOLEAN from source field '.+' of type TINYINT")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of BOOLEAN type, but TINYINT was found"),
                TestParams.passingCase(1303, TINYINT, TINYINT, "cast(42 as tinyint)", (byte) 42, (byte) 42),
                TestParams.passingCase(1304, TINYINT, SMALLINT, "cast(42 as tinyint)", (byte) 42, (short) 42),
                TestParams.passingCase(1305, TINYINT, INTEGER, "cast(42 as tinyint)", (byte) 42, 42),
                TestParams.passingCase(1306, TINYINT, BIGINT, "cast(42 as tinyint)", (byte) 42, 42L),
                TestParams.passingCase(1307, TINYINT, DECIMAL, "cast(42 as tinyint)", (byte) 42, BigDecimal.valueOf(42)),
                TestParams.passingCase(1308, TINYINT, REAL, "cast(42 as tinyint)", (byte) 42, 42F),
                TestParams.passingCase(1309, TINYINT, DOUBLE, "cast(42 as tinyint)", (byte) 42, 42D),
                TestParams.failingCase(1310, TINYINT, TIME, "cast(42 as tinyint)", (byte) 42)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type TIME from source field '.+' of type TINYINT")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TIME from source field '.+' of type TINYINT")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TIME type, but TINYINT was found"),
                TestParams.failingCase(1311, TINYINT, DATE, "cast(42 as tinyint)", (byte) 42)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type DATE from source field '.+' of type TINYINT")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type DATE from source field '.+' of type TINYINT")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of DATE type, but TINYINT was found"),
                TestParams.failingCase(1312, TINYINT, TIMESTAMP, "cast(42 as tinyint)", (byte) 42)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type TIMESTAMP from source field '.+' of type TINYINT")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TIMESTAMP from source field '.+' of type TINYINT")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TIMESTAMP type, but TINYINT was found"),
                TestParams.failingCase(1313, TINYINT, TIMESTAMP_WITH_TIME_ZONE, "cast(42 as tinyint)", (byte) 42)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type TIMESTAMP WITH TIME ZONE from source field '.+' of type TINYINT")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TIMESTAMP WITH TIME ZONE from source field '.+' of type TINYINT")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TIMESTAMP WITH TIME ZONE type, but TINYINT was found"),
                TestParams.passingCase(1314, TINYINT, OBJECT, "cast(42 as tinyint)", (byte) 42, (byte) 42),
                TestParams.failingCase(1315, TINYINT, JSON, "cast(42 as tinyint)", (byte) 42)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type JSON from source field '.+' of type TINYINT")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type JSON from source field '.+' of type TINYINT")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of JSON type, but TINYINT was found"),

                // SMALLINT
                TestParams.failingCase(1401, SMALLINT, VARCHAR, "cast(42 as smallint)", (short) 42)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type VARCHAR from source field '.+' of type SMALLINT")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type VARCHAR from source field '.+' of type SMALLINT")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of VARCHAR type, but SMALLINT was found"),
                TestParams.failingCase(1402, SMALLINT, BOOLEAN, "cast(42 as smallint)", (short) 42)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type BOOLEAN from source field '.+' of type SMALLINT")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type BOOLEAN from source field '.+' of type SMALLINT")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of BOOLEAN type, but SMALLINT was found"),
                TestParams.passingCase(1403, SMALLINT, TINYINT, "cast(42 as smallint)", (short) 42, (byte) 42),
                TestParams.failingCase(1404, SMALLINT, TINYINT, "420", (short) 420)
                        .withExpectedFailureRegex("Numeric overflow while converting SMALLINT to TINYINT"),
                TestParams.passingCase(1405, SMALLINT, SMALLINT, "cast(42 as smallint)", (short) 42, (short) 42),
                TestParams.passingCase(1406, SMALLINT, INTEGER, "cast(42 as smallint)", (short) 42, 42),
                TestParams.passingCase(1407, SMALLINT, BIGINT, "cast(42 as smallint)", (short) 42, 42L),
                TestParams.passingCase(1408, SMALLINT, DECIMAL, "cast(42 as smallint)", (short) 42, BigDecimal.valueOf(42)),
                TestParams.passingCase(1409, SMALLINT, REAL, "cast(42 as smallint)", (short) 42, 42F),
                TestParams.passingCase(1410, SMALLINT, DOUBLE, "cast(42 as smallint)", (short) 42, 42D),
                TestParams.failingCase(1411, SMALLINT, TIME, "cast(42 as smallint)", (short) 42)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type TIME from source field '.+' of type SMALLINT")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TIME from source field '.+' of type SMALLINT")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TIME type, but SMALLINT was found"),
                TestParams.failingCase(1412, SMALLINT, DATE, "cast(42 as smallint)", (short) 42)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type DATE from source field '.+' of type SMALLINT")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type DATE from source field '.+' of type SMALLINT")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of DATE type, but SMALLINT was found"),
                TestParams.failingCase(1413, SMALLINT, TIMESTAMP, "cast(42 as smallint)", (short) 42)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type TIMESTAMP from source field '.+' of type SMALLINT")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TIMESTAMP from source field '.+' of type SMALLINT")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TIMESTAMP type, but SMALLINT was found"),
                TestParams.failingCase(1414, SMALLINT, TIMESTAMP_WITH_TIME_ZONE, "cast(42 as smallint)", (short) 42)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type TIMESTAMP WITH TIME ZONE from source field '.+' of type SMALLINT")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TIMESTAMP WITH TIME ZONE from source field '.+' of type SMALLINT")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TIMESTAMP WITH TIME ZONE type, but SMALLINT was found"),
                TestParams.passingCase(1415, SMALLINT, OBJECT, "cast(42 as smallint)", (short) 42, (short) 42),
                TestParams.failingCase(1416, SMALLINT, JSON, "cast(42 as smallint)", (short) 42)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type JSON from source field '.+' of type SMALLINT")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type JSON from source field '.+' of type SMALLINT")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of JSON type, but SMALLINT was found"),

                // INTEGER
                TestParams.failingCase(1501, INTEGER, VARCHAR, "cast(42 as integer)", 42)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type VARCHAR from source field '.+' of type INTEGER")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type VARCHAR from source field '.+' of type INTEGER")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of VARCHAR type, but INTEGER was found"),
                TestParams.failingCase(1502, INTEGER, BOOLEAN, "cast(42 as integer)", 42)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type BOOLEAN from source field '.+' of type INTEGER")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type BOOLEAN from source field '.+' of type INTEGER")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of BOOLEAN type, but INTEGER was found"),
                TestParams.passingCase(1503, INTEGER, TINYINT, "cast(42 as integer)", 42, (byte) 42),
                TestParams.failingCase(1504, INTEGER, TINYINT, "42000", 42000)
                        .withExpectedFailureRegex("Numeric overflow while converting INTEGER to TINYINT"),
                TestParams.passingCase(1505, INTEGER, SMALLINT, "cast(42 as integer)", 42, (short) 42),
                TestParams.failingCase(1506, INTEGER, SMALLINT, "42000", 42000)
                        .withExpectedFailureRegex("Numeric overflow while converting INTEGER to SMALLINT"),
                TestParams.passingCase(1507, INTEGER, INTEGER, "cast(42 as integer)", 42, 42),
                TestParams.passingCase(1508, INTEGER, BIGINT, "cast(42 as integer)", 42, 42L),
                TestParams.passingCase(1509, INTEGER, DECIMAL, "cast(42 as integer)", 42, BigDecimal.valueOf(42)),
                TestParams.passingCase(1510, INTEGER, REAL, "cast(42 as integer)", 42, 42F),
                TestParams.passingCase(1511, INTEGER, DOUBLE, "cast(42 as integer)", 42, 42D),
                TestParams.failingCase(1512, INTEGER, TIME, "cast(42 as integer)", 42)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type TIME from source field '.+' of type INTEGER")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TIME from source field '.+' of type INTEGER")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TIME type, but INTEGER was found"),
                TestParams.failingCase(1513, INTEGER, DATE, "cast(42 as integer)", 42)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type DATE from source field '.+' of type INTEGER")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type DATE from source field '.+' of type INTEGER")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of DATE type, but INTEGER was found"),
                TestParams.failingCase(1514, INTEGER, TIMESTAMP, "cast(42 as integer)", 42)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type TIMESTAMP from source field '.+' of type INTEGER")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TIMESTAMP from source field '.+' of type INTEGER")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TIMESTAMP type, but INTEGER was found"),
                TestParams.failingCase(1515, INTEGER, TIMESTAMP_WITH_TIME_ZONE, "cast(42 as integer)", 42)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type TIMESTAMP WITH TIME ZONE from source field '.+' of type INTEGER")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TIMESTAMP WITH TIME ZONE from source field '.+' of type INTEGER")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TIMESTAMP WITH TIME ZONE type, but INTEGER was found"),
                TestParams.passingCase(1516, INTEGER, OBJECT, "cast(42 as integer)", 42, 42),
                TestParams.failingCase(1517, INTEGER, JSON, "cast(42 as integer)", 42)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type JSON from source field '.+' of type INTEGER")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type JSON from source field '.+' of type INTEGER")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of JSON type, but INTEGER was found"),

                // BIGINT
                TestParams.failingCase(1601, BIGINT, VARCHAR, "cast(42 as bigint)", 42L)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type VARCHAR from source field '.+' of type BIGINT")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type VARCHAR from source field '.+' of type BIGINT")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of VARCHAR type, but BIGINT was found"),
                TestParams.failingCase(1602, BIGINT, BOOLEAN, "cast(42 as bigint)", 42L)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type BOOLEAN from source field '.+' of type BIGINT")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type BOOLEAN from source field '.+' of type BIGINT")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of BOOLEAN type, but BIGINT was found"),
                TestParams.passingCase(1603, BIGINT, TINYINT, "cast(42 as bigint)", 42L, (byte) 42),
                TestParams.failingCase(1604, BIGINT, TINYINT, "4200000000", 4200000000L)
                        .withExpectedFailureRegex("Numeric overflow while converting BIGINT to TINYINT"),
                TestParams.passingCase(1605, BIGINT, SMALLINT, "cast(42 as bigint)", 42L, (short) 42),
                TestParams.failingCase(1606, BIGINT, SMALLINT, "4200000000", 4200000000L)
                        .withExpectedFailureRegex("Numeric overflow while converting BIGINT to SMALLINT"),
                TestParams.passingCase(1607, BIGINT, INTEGER, "cast(42 as bigint)", 42L, 42),
                TestParams.failingCase(1608, BIGINT, INTEGER, "4200000000", 4200000000L)
                        .withExpectedFailureRegex("Numeric overflow while converting BIGINT to INTEGER"),
                TestParams.passingCase(1609, BIGINT, BIGINT, "cast(42 as bigint)", 42L, 42L),
                TestParams.passingCase(1610, BIGINT, DECIMAL, "cast(42 as bigint)", 42L, BigDecimal.valueOf(42)),
                TestParams.passingCase(1611, BIGINT, REAL, "cast(42 as bigint)", 42L, 42F),
                TestParams.passingCase(1612, BIGINT, DOUBLE, "cast(42 as bigint)", 42L, 42D),
                TestParams.failingCase(1613, BIGINT, TIME, "cast(42 as bigint)", 42L)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type TIME from source field '.+' of type BIGINT")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TIME from source field '.+' of type BIGINT")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TIME type, but BIGINT was found"),
                TestParams.failingCase(1614, BIGINT, DATE, "cast(42 as bigint)", 42L)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type DATE from source field '.+' of type BIGINT")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type DATE from source field '.+' of type BIGINT")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of DATE type, but BIGINT was found"),
                TestParams.failingCase(1615, BIGINT, TIMESTAMP, "cast(42 as bigint)", 42L)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type TIMESTAMP from source field '.+' of type BIGINT")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TIMESTAMP from source field '.+' of type BIGINT")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TIMESTAMP type, but BIGINT was found"),
                TestParams.failingCase(1616, BIGINT, TIMESTAMP_WITH_TIME_ZONE, "cast(42 as bigint)", 42L)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type TIMESTAMP WITH TIME ZONE from source field '.+' of type BIGINT")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TIMESTAMP WITH TIME ZONE from source field '.+' of type BIGINT")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TIMESTAMP WITH TIME ZONE type, but BIGINT was found"),
                TestParams.passingCase(1617, BIGINT, OBJECT, "cast(42 as bigint)", 42L, 42L),
                TestParams.failingCase(1618, BIGINT, JSON, "cast(42 as bigint)", 42L)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type JSON from source field '.+' of type BIGINT")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type JSON from source field '.+' of type BIGINT")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of JSON type, but BIGINT was found"),

                // DECIMAL
                TestParams.failingCase(1701, DECIMAL, VARCHAR, "cast(42 as decimal)", new BigDecimal("42"))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type VARCHAR from source field '.+' of type DECIMAL\\(76, 38\\)")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type VARCHAR from source field '.+' of type DECIMAL\\(76, 38\\)")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of VARCHAR type, but DECIMAL was found"),
                TestParams.failingCase(1702, DECIMAL, BOOLEAN, "cast(42 as decimal)", new BigDecimal("42"))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type BOOLEAN from source field '.+' of type DECIMAL\\(76, 38\\)")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type BOOLEAN from source field '.+' of type DECIMAL\\(76, 38\\)")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of BOOLEAN type, but DECIMAL was found"),
                TestParams.passingCase(1703, DECIMAL, TINYINT, "cast(42 as decimal)", new BigDecimal("42"), (byte) 42)
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TINYINT type, but DECIMAL was found"),
                TestParams.failingCase(1704, DECIMAL, TINYINT, "9223372036854775809", new BigDecimal("9223372036854775809"))
                        .withExpectedLiteralFailureRegex("Numeric overflow while converting DECIMAL to TINYINT")
                        .withExpectedColumnFailureRegex("Numeric overflow while converting DECIMAL to TINYINT")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TINYINT type, but DECIMAL was found"),
                TestParams.passingCase(1705, DECIMAL, SMALLINT, "cast(42 as decimal)", new BigDecimal("42"), (short) 42)
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of SMALLINT type, but DECIMAL was found"),
                TestParams.failingCase(1706, DECIMAL, SMALLINT, "9223372036854775809", new BigDecimal("9223372036854775809"))
                        .withExpectedLiteralFailureRegex("Numeric overflow while converting DECIMAL to SMALLINT")
                        .withExpectedColumnFailureRegex("Numeric overflow while converting DECIMAL to SMALLINT")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of SMALLINT type, but DECIMAL was found"),
                TestParams.passingCase(1707, DECIMAL, INTEGER, "cast(42 as decimal)", new BigDecimal("42"), 42)
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of INTEGER type, but DECIMAL was found"),
                TestParams.failingCase(1708, DECIMAL, INTEGER, "9223372036854775809", new BigDecimal("9223372036854775809"))
                        .withExpectedLiteralFailureRegex("Numeric overflow while converting DECIMAL to INTEGER")
                        .withExpectedColumnFailureRegex("Numeric overflow while converting DECIMAL to INTEGER")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of INTEGER type, but DECIMAL was found"),
                TestParams.passingCase(1709, DECIMAL, BIGINT, "cast(42 as decimal)", new BigDecimal("42"), 42L)
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of BIGINT type, but DECIMAL was found"),
                TestParams.passingCase(1710, DECIMAL, BIGINT, "cast(42.1 as decimal)", new BigDecimal("42.1"), 42L)
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of BIGINT type, but DECIMAL was found"),
                TestParams.failingCase(1711, DECIMAL, BIGINT, "9223372036854775809", new BigDecimal("9223372036854775809"))
                        .withExpectedLiteralFailureRegex("Numeric overflow while converting DECIMAL to BIGINT")
                        .withExpectedColumnFailureRegex("Numeric overflow while converting DECIMAL to BIGINT")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of BIGINT type, but DECIMAL was found"),
                TestParams.passingCase(1712, DECIMAL, DECIMAL, "cast(42 as decimal)", new BigDecimal("42"), BigDecimal.valueOf(42)),
                TestParams.passingCase(1713, DECIMAL, REAL, "cast(42 as decimal)", new BigDecimal("42"), 42F),
                TestParams.passingCase(1714, DECIMAL, DOUBLE, "cast(42 as decimal)", new BigDecimal("42"), 42D),
                TestParams.failingCase(1715, DECIMAL, TIME, "cast(42 as decimal)", new BigDecimal("42"))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type TIME from source field '.+' of type DECIMAL")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TIME from source field '.+' of type DECIMAL")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TIME type, but DECIMAL was found"),
                TestParams.failingCase(1716, DECIMAL, DATE, "cast(42 as decimal)", new BigDecimal("42"))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type DATE from source field '.+' of type DECIMAL")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type DATE from source field '.+' of type DECIMAL")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of DATE type, but DECIMAL was found"),
                TestParams.failingCase(1717, DECIMAL, TIMESTAMP, "cast(42 as decimal)", new BigDecimal("42"))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type TIMESTAMP from source field '.+' of type DECIMAL\\(76, 38\\)")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TIMESTAMP from source field '.+' of type DECIMAL\\(76, 38\\)")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TIMESTAMP type, but DECIMAL was found"),
                TestParams.failingCase(1718, DECIMAL, TIMESTAMP_WITH_TIME_ZONE, "cast(42 as decimal)", new BigDecimal("42"))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type TIMESTAMP WITH TIME ZONE from source field '.+' of type DECIMAL\\(76, 38\\)")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TIMESTAMP WITH TIME ZONE from source field '.+' of type DECIMAL\\(76, 38\\)")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TIMESTAMP WITH TIME ZONE type, but DECIMAL was found"),
                TestParams.passingCase(1719, DECIMAL, OBJECT, "cast(42 as decimal)", new BigDecimal("42"), BigDecimal.valueOf(42)),
                TestParams.failingCase(1720, DECIMAL, JSON, "cast(42 as decimal)", new BigDecimal("42"))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type JSON from source field '.+' of type DECIMAL")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type JSON from source field '.+' of type DECIMAL")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of JSON type, but DECIMAL was found"),

                // REAL
                TestParams.failingCase(1801, REAL, VARCHAR, "cast(42 as real)", 42F)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type VARCHAR from source field '.+' of type REAL")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type VARCHAR from source field '.+' of type REAL")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of VARCHAR type, but REAL was found"),
                TestParams.failingCase(1802, REAL, BOOLEAN, "cast(42 as real)", 42F)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type BOOLEAN from source field '.+' of type REAL")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type BOOLEAN from source field '.+' of type REAL")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of BOOLEAN type, but REAL was found"),
                TestParams.passingCase(1803, REAL, TINYINT, "cast(42 as real)", 42F, (byte) 42)
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TINYINT type, but REAL was found"),
                TestParams.failingCase(1804, REAL, TINYINT, "cast(420 as real)", 420F)
                        .withExpectedLiteralFailureRegex("Numeric overflow while converting REAL to TINYINT")
                        .withExpectedColumnFailureRegex("Numeric overflow while converting REAL to TINYINT")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TINYINT type, but REAL was found"),
                TestParams.passingCase(1805, REAL, SMALLINT, "cast(42 as real)", 42F, (short) 42)
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of SMALLINT type, but REAL was found"),
                TestParams.failingCase(1806, REAL, SMALLINT, "cast(420000 as real)", 420000F)
                        .withExpectedLiteralFailureRegex("Numeric overflow while converting REAL to SMALLINT")
                        .withExpectedColumnFailureRegex("Numeric overflow while converting REAL to SMALLINT")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of SMALLINT type, but REAL was found"),
                TestParams.passingCase(1807, REAL, INTEGER, "cast(42 as real)", 42F, 42)
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of INTEGER type, but REAL was found"),
                TestParams.failingCase(1808, REAL, INTEGER, "cast(4200000000 as real)", 4200000000F)
                        .withExpectedLiteralFailureRegex("Numeric overflow while converting REAL to INTEGER")
                        .withExpectedColumnFailureRegex("Numeric overflow while converting REAL to INTEGER")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of INTEGER type, but REAL was found"),
                TestParams.passingCase(1809, REAL, BIGINT, "cast(42 as real)", 42F, 42L)
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of BIGINT type, but REAL was found"),
                TestParams.passingCase(1810, REAL, BIGINT, "cast(42.1 as real)", 42.1F, 42L)
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of BIGINT type, but REAL was found"),
                TestParams.failingCase(1811, REAL, BIGINT, "cast(18223372036854775808000 as real)", 18223372036854775808000F)
                        .withExpectedLiteralFailureRegex("Numeric overflow while converting REAL to BIGINT")
                        .withExpectedColumnFailureRegex("Numeric overflow while converting REAL to BIGINT")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of BIGINT type, but REAL was found"),
                TestParams.passingCase(1812, REAL, DECIMAL, "cast(42 as real)", 42F, BigDecimal.valueOf(42))
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of DECIMAL type, but REAL was found"),
                TestParams.passingCase(1813, REAL, REAL, "cast(42 as real)", 42F, 42F),
                TestParams.passingCase(1814, REAL, DOUBLE, "cast(42 as real)", 42F, 42D),
                TestParams.failingCase(1815, REAL, TIME, "cast(42 as real)", 42F)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type TIME from source field '.+' of type REAL")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TIME from source field '.+' of type REAL")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TIME type, but REAL was found"),
                TestParams.failingCase(1816, REAL, DATE, "cast(42 as real)", 42F)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type DATE from source field '.+' of type REAL")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type DATE from source field '.+' of type REAL")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of DATE type, but REAL was found"),
                TestParams.failingCase(1817, REAL, TIMESTAMP, "cast(42 as real)", 42F)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type TIMESTAMP from source field '.+' of type REAL")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TIMESTAMP from source field '.+' of type REAL")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TIMESTAMP type, but REAL was found"),
                TestParams.failingCase(1818, REAL, TIMESTAMP_WITH_TIME_ZONE, "cast(42 as real)", 42F)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type TIMESTAMP WITH TIME ZONE from source field '.+' of type REAL")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TIMESTAMP WITH TIME ZONE from source field '.+' of type REAL")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TIMESTAMP WITH TIME ZONE type, but REAL was found"),
                TestParams.passingCase(1819, REAL, OBJECT, "cast(42 as real)", 42F, 42F),
                TestParams.failingCase(1820, REAL, JSON, "cast(42 as real)", 42F)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type JSON from source field '.+' of type REAL")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type JSON from source field '.+' of type REAL")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of JSON type, but REAL was found"),

                // DOUBLE
                TestParams.failingCase(1901, DOUBLE, VARCHAR, "cast(42 as double)", 42D)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type VARCHAR from source field '.+' of type DOUBLE")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type VARCHAR from source field '.+' of type DOUBLE")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of VARCHAR type, but DOUBLE was found"),
                TestParams.failingCase(1902, DOUBLE, BOOLEAN, "cast(42 as double)", 42D)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type BOOLEAN from source field '.+' of type DOUBLE")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type BOOLEAN from source field '.+' of type DOUBLE")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of BOOLEAN type, but DOUBLE was found"),
                TestParams.passingCase(1903, DOUBLE, TINYINT, "cast(42 as double)", 42D, (byte) 42)
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TINYINT type, but DOUBLE was found"),
                TestParams.failingCase(1904, DOUBLE, TINYINT, "cast(420 as double)", 420D)
                        .withExpectedLiteralFailureRegex("Numeric overflow while converting DOUBLE to TINYINT")
                        .withExpectedColumnFailureRegex("Numeric overflow while converting DOUBLE to TINYINT")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TINYINT type, but DOUBLE was found"),
                TestParams.passingCase(1905, DOUBLE, SMALLINT, "cast(42 as double)", 42D, (short) 42)
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of SMALLINT type, but DOUBLE was found"),
                TestParams.failingCase(1906, DOUBLE, SMALLINT, "cast(420000 as double)", 420000D)
                        .withExpectedLiteralFailureRegex("Numeric overflow while converting DOUBLE to SMALLINT")
                        .withExpectedColumnFailureRegex("Numeric overflow while converting DOUBLE to SMALLINT")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of SMALLINT type, but DOUBLE was found"),
                TestParams.passingCase(1907, DOUBLE, INTEGER, "cast(42 as double)", 42D, 42)
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of INTEGER type, but DOUBLE was found"),
                TestParams.failingCase(1908, DOUBLE, INTEGER, "cast(4200000000 as double)", 4200000000D)
                        .withExpectedLiteralFailureRegex("Numeric overflow while converting DOUBLE to INTEGER")
                        .withExpectedColumnFailureRegex("Numeric overflow while converting DOUBLE to INTEGER")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of INTEGER type, but DOUBLE was found"),
                TestParams.passingCase(1909, DOUBLE, BIGINT, "cast(42 as double)", 42D, 42L)
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of BIGINT type, but DOUBLE was found"),
                TestParams.failingCase(1910, DOUBLE, BIGINT, "cast(19223372036854775808000 as double)", 19223372036854775808000D)
                        .withExpectedLiteralFailureRegex("Numeric overflow while converting DOUBLE to BIGINT")
                        .withExpectedColumnFailureRegex("Numeric overflow while converting DOUBLE to BIGINT")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of BIGINT type, but DOUBLE was found"),
                TestParams.passingCase(1911, DOUBLE, DECIMAL, "cast(42 as double)", 42D, BigDecimal.valueOf(42))
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of DECIMAL type, but DOUBLE was found"),
                TestParams.passingCase(1912, DOUBLE, REAL, "cast(42 as double)", 42D, 42F)
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of REAL type, but DOUBLE was found"),
                TestParams.passingCase(1913, DOUBLE, BIGINT, "cast(42.1 as double)", 42.1D, 42L)
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of BIGINT type, but DOUBLE was found"),
                TestParams.failingCase(1914, DOUBLE, REAL, "cast(42e42 as double)", 42e42D)
                        .withExpectedLiteralFailureRegex("Numeric overflow while converting DOUBLE to REAL")
                        .withExpectedColumnFailureRegex("Numeric overflow while converting DOUBLE to REAL")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of REAL type, but DOUBLE was found"),
                TestParams.passingCase(1915, DOUBLE, DOUBLE, "cast(42 as double)", 42D, 42D),
                TestParams.failingCase(1916, DOUBLE, TIME, "cast(42 as double)", 42D)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type TIME from source field '.+' of type DOUBLE")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TIME from source field '.+' of type DOUBLE")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TIME type, but DOUBLE was found"),
                TestParams.failingCase(1917, DOUBLE, DATE, "cast(42 as double)", 42D)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type DATE from source field '.+' of type DOUBLE")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type DATE from source field '.+' of type DOUBLE")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of DATE type, but DOUBLE was found"),
                TestParams.failingCase(1918, DOUBLE, TIMESTAMP, "cast(42 as double)", 42D)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type TIMESTAMP from source field '.+' of type DOUBLE")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TIMESTAMP from source field '.+' of type DOUBLE")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TIMESTAMP type, but DOUBLE was found"),
                TestParams.failingCase(1919, DOUBLE, TIMESTAMP_WITH_TIME_ZONE, "cast(42 as double)", 42D)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type TIMESTAMP WITH TIME ZONE from source field '.+' of type DOUBLE")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TIMESTAMP WITH TIME ZONE from source field '.+' of type DOUBLE")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TIMESTAMP WITH TIME ZONE type, but DOUBLE was found"),
                TestParams.passingCase(1920, DOUBLE, OBJECT, "cast(42 as double)", 42D, 42D),
                TestParams.failingCase(1921, DOUBLE, JSON, "cast(42 as double)", 42D)
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type JSON from source field '.+' of type DOUBLE")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type JSON from source field '.+' of type DOUBLE")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of JSON type, but DOUBLE was found"),

                // TIME
                TestParams.failingCase(2001, TIME, VARCHAR, "cast('01:42:00' as time)", LocalTime.of(1, 42, 0))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type VARCHAR from source field '.+' of type TIME")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type VARCHAR from source field '.+' of type TIME")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of VARCHAR type, but TIME was found"),
                TestParams.failingCase(2002, TIME, BOOLEAN, "cast('01:42:00' as time)", LocalTime.of(1, 42, 0))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type BOOLEAN from source field '.+' of type TIME")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type BOOLEAN from source field '.+' of type TIME")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of BOOLEAN type, but TIME was found"),
                TestParams.failingCase(2003, TIME, TINYINT, "cast('1:42:00' as time)", LocalTime.of(1, 42, 0))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type TINYINT from source field '.+' of type TIME")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TINYINT from source field '.+' of type TIME")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TINYINT type, but TIME was found"),
                TestParams.failingCase(2004, TIME, SMALLINT, "cast('1:42:00' as time)", LocalTime.of(1, 42, 0))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type SMALLINT from source field '.+' of type TIME")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type SMALLINT from source field '.+' of type TIME")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of SMALLINT type, but TIME was found"),
                TestParams.failingCase(2005, TIME, INTEGER, "cast('1:42:00' as time)", LocalTime.of(1, 42, 0))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type INTEGER from source field '.+' of type TIME")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type INTEGER from source field '.+' of type TIME")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of INTEGER type, but TIME was found"),
                TestParams.failingCase(2006, TIME, BIGINT, "cast('1:42:00' as time)", LocalTime.of(1, 42, 0))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type BIGINT from source field '.+' of type TIME")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type BIGINT from source field '.+' of type TIME")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of BIGINT type, but TIME was found"),
                TestParams.failingCase(2007, TIME, DECIMAL, "cast('1:42:00' as time)", LocalTime.of(1, 42, 0))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type DECIMAL\\(76, 38\\) from source field '.+' of type TIME")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type DECIMAL\\(76, 38\\) from source field '.+' of type TIME")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of DECIMAL type, but TIME was found"),
                TestParams.failingCase(2008, TIME, REAL, "cast('1:42:00' as time)", LocalTime.of(1, 42, 0))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type REAL from source field '.+' of type TIME")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type REAL from source field '.+' of type TIME")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of REAL type, but TIME was found"),
                TestParams.failingCase(2009, TIME, DOUBLE, "cast('1:42:00' as time)", LocalTime.of(1, 42, 0))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type DOUBLE from source field '.+' of type TIME")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type DOUBLE from source field '.+' of type TIME")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of DOUBLE type, but TIME was found"),
                TestParams.passingCase(2010, TIME, TIME, "cast('01:42:00' as time)", LocalTime.of(1, 42, 0),
                        LocalTime.of(1, 42)),
                TestParams.failingCase(2011, TIME, DATE, "cast('01:42:00' as time)", LocalTime.of(1, 42, 0))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type DATE from source field '.+' of type TIME")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type DATE from source field '.+' of type TIME")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of DATE type, but TIME was found"),
                TestParams.passingCase(2012, TIME, TIMESTAMP, "cast('01:42:00' as time)", LocalTime.of(1, 42, 0),
                        LocalDateTime.of(TODAY, LocalTime.of(1, 42)),
                        LocalDateTime.of(TOMORROW, LocalTime.of(1, 42))),
                TestParams.passingCase(2013, TIME, TIMESTAMP_WITH_TIME_ZONE, "cast('01:42:00' as time)", LocalTime.of(1, 42, 0),
                        ZonedDateTime.of(LocalDateTime.of(TODAY, LocalTime.of(1, 42)), DEFAULT_ZONE).toOffsetDateTime(),
                        ZonedDateTime.of(LocalDateTime.of(TOMORROW, LocalTime.of(1, 42)), DEFAULT_ZONE).toOffsetDateTime()),
                TestParams.passingCase(2014, TIME, OBJECT, "cast('01:42:00' as time)", LocalTime.of(1, 42, 0), LocalTime.of(1, 42)),
                TestParams.failingCase(2015, TIME, JSON, "cast('01:42:00' as time)", LocalTime.of(1, 42, 0))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type JSON from source field '.+' of type TIME")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type JSON from source field '.+' of type TIME")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of JSON type, but TIME was found"),

                // DATE
                TestParams.failingCase(2101, DATE, VARCHAR, "cast('2020-12-30' as date)", LocalDate.of(2020, 12, 30))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type VARCHAR from source field '.+' of type DATE")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type VARCHAR from source field '.+' of type DATE")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of VARCHAR type, but DATE was found"),
                TestParams.failingCase(2102, DATE, BOOLEAN, "cast('2020-12-30' as date)", LocalDate.of(2020, 12, 30))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type BOOLEAN from source field '.+' of type DATE")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type BOOLEAN from source field '.+' of type DATE")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of BOOLEAN type, but DATE was found"),
                TestParams.failingCase(2103, DATE, TINYINT, "cast('2020-12-30' as date)", LocalDate.of(2020, 12, 30))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type TINYINT from source field '.+' of type DATE")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TINYINT from source field '.+' of type DATE")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TINYINT type, but DATE was found"),
                TestParams.failingCase(2104, DATE, SMALLINT, "cast('2020-12-30' as date)", LocalDate.of(2020, 12, 30))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type SMALLINT from source field '.+' of type DATE")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type SMALLINT from source field '.+' of type DATE")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of SMALLINT type, but DATE was found"),
                TestParams.failingCase(2105, DATE, INTEGER, "cast('2020-12-30' as date)", LocalDate.of(2020, 12, 30))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type INTEGER from source field '.+' of type DATE")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type INTEGER from source field '.+' of type DATE")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of INTEGER type, but DATE was found"),
                TestParams.failingCase(2106, DATE, BIGINT, "cast('2020-12-30' as date)", LocalDate.of(2020, 12, 30))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type BIGINT from source field '.+' of type DATE")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type BIGINT from source field '.+' of type DATE")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of BIGINT type, but DATE was found"),
                TestParams.failingCase(2107, DATE, DECIMAL, "cast('2020-12-30' as date)", LocalDate.of(2020, 12, 30))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type DECIMAL\\(76, 38\\) from source field '.+' of type DATE")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type DECIMAL\\(76, 38\\) from source field '.+' of type DATE")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of DECIMAL type, but DATE was found"),
                TestParams.failingCase(2108, DATE, REAL, "cast('2020-12-30' as date)", LocalDate.of(2020, 12, 30))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type REAL from source field '.+' of type DATE")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type REAL from source field '.+' of type DATE")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of REAL type, but DATE was found"),
                TestParams.failingCase(2109, DATE, DOUBLE, "cast('2020-12-30' as date)", LocalDate.of(2020, 12, 30))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type DOUBLE from source field '.+' of type DATE")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type DOUBLE from source field '.+' of type DATE")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of DOUBLE type, but DATE was found"),
                TestParams.failingCase(2110, DATE, TIME, "cast('2020-12-30' as date)", LocalDate.of(2020, 12, 30))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type TIME from source field '.+' of type DATE")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TIME from source field '.+' of type DATE")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TIME type, but DATE was found"),
                TestParams.passingCase(2111, DATE, DATE, "cast('2020-12-30' as date)", LocalDate.of(2020, 12, 30),
                        LocalDate.of(2020, 12, 30)),
                TestParams.passingCase(2112, DATE, TIMESTAMP, "cast('2020-12-30' as date)", LocalDate.of(2020, 12, 30),
                        LocalDateTime.of(2020, 12, 30, 0, 0)),
                TestParams.passingCase(2113, DATE, TIMESTAMP_WITH_TIME_ZONE, "cast('2020-12-30' as date)",
                        LocalDate.of(2020, 12, 30),
                        ZonedDateTime.of(2020, 12, 30, 0, 0, 0, 0, DEFAULT_ZONE).toOffsetDateTime()),
                TestParams.passingCase(2114, DATE, OBJECT, "cast('2020-12-30' as date)",
                        LocalDate.of(2020, 12, 30),
                        LocalDate.of(2020, 12, 30)),
                TestParams.failingCase(2021, DATE, JSON, "cast('2020-12-30' as date)", LocalDate.of(2020, 12, 30))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type JSON from source field '.+' of type DATE")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type JSON from source field '.+' of type DATE")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of JSON type, but DATE was found"),

                // TIMESTAMP
                TestParams.failingCase(2201, TIMESTAMP, VARCHAR, "cast('2020-12-30T01:42:00' as timestamp)", LocalDateTime.of(2020, 12, 30, 1, 42, 0))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type VARCHAR from source field '.+' of type TIMESTAMP")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type VARCHAR from source field '.+' of type TIMESTAMP")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of VARCHAR type, but TIMESTAMP was found"),
                TestParams.failingCase(2202, TIMESTAMP, BOOLEAN, "cast('2020-12-30T01:42:00' as timestamp)", LocalDateTime.of(2020, 12, 30, 1, 42, 0))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type BOOLEAN from source field '.+' of type TIMESTAMP")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type BOOLEAN from source field '.+' of type TIMESTAMP")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of BOOLEAN type, but TIMESTAMP was found"),
                TestParams.failingCase(2203, TIMESTAMP, TINYINT, "cast('2020-12-30T01:42:00' as timestamp)", LocalDateTime.of(2020, 12, 30, 1, 42, 0))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type TINYINT from source field '.+' of type TIMESTAMP")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TINYINT from source field '.+' of type TIMESTAMP")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TINYINT type, but TIMESTAMP was found"),
                TestParams.failingCase(2204, TIMESTAMP, SMALLINT, "cast('2020-12-30T01:42:00' as timestamp)", LocalDateTime.of(2020, 12, 30, 1, 42, 0))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type SMALLINT from source field '.+' of type TIMESTAMP")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type SMALLINT from source field '.+' of type TIMESTAMP")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of SMALLINT type, but TIMESTAMP was found"),
                TestParams.failingCase(2205, TIMESTAMP, INTEGER, "cast('2020-12-30T01:42:00' as timestamp)", LocalDateTime.of(2020, 12, 30, 1, 42, 0))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type INTEGER from source field '.+' of type TIMESTAMP")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type INTEGER from source field '.+' of type TIMESTAMP")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of INTEGER type, but TIMESTAMP was found"),
                TestParams.failingCase(2206, TIMESTAMP, BIGINT, "cast('2020-12-30T01:42:00' as timestamp)", LocalDateTime.of(2020, 12, 30, 1, 42, 0))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type BIGINT from source field '.+' of type TIMESTAMP")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type BIGINT from source field '.+' of type TIMESTAMP")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of BIGINT type, but TIMESTAMP was found"),
                TestParams.failingCase(2207, TIMESTAMP, DECIMAL, "cast('2020-12-30T01:42:00' as timestamp)", LocalDateTime.of(2020, 12, 30, 1, 42, 0))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type DECIMAL\\(76, 38\\) from source field '.+' of type TIMESTAMP")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type DECIMAL\\(76, 38\\) from source field '.+' of type TIMESTAMP")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of DECIMAL type, but TIMESTAMP was found"),
                TestParams.failingCase(2208, TIMESTAMP, REAL, "cast('2020-12-30T01:42:00' as timestamp)", LocalDateTime.of(2020, 12, 30, 1, 42, 0))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type REAL from source field '.+' of type TIMESTAMP")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type REAL from source field '.+' of type TIMESTAMP")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of REAL type, but TIMESTAMP was found"),
                TestParams.failingCase(2209, TIMESTAMP, DOUBLE, "cast('2020-12-30T01:42:00' as timestamp)", LocalDateTime.of(2020, 12, 30, 1, 42, 0))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type DOUBLE from source field '.+' of type TIMESTAMP")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type DOUBLE from source field '.+' of type TIMESTAMP")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of DOUBLE type, but TIMESTAMP was found"),
                TestParams.passingCase(2210, TIMESTAMP, TIME, "cast('2020-12-30T01:42:00' as timestamp)", LocalDateTime.of(2020, 12, 30, 1, 42, 0),
                        LocalTime.of(1, 42))
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TIME type, but TIMESTAMP was found"),
                TestParams.passingCase(2211, TIMESTAMP, DATE, "cast('2020-12-30T01:42:00' as timestamp)", LocalDateTime.of(2020, 12, 30, 1, 42, 0),
                        LocalDate.of(2020, 12, 30))
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of DATE type, but TIMESTAMP was found"),
                TestParams.passingCase(2212, TIMESTAMP, TIMESTAMP, "cast('2020-12-30T01:42:00' as timestamp)", LocalDateTime.of(2020, 12, 30, 1, 42, 0),
                        LocalDateTime.of(2020, 12, 30, 1, 42)),
                TestParams.passingCase(2213, TIMESTAMP, TIMESTAMP_WITH_TIME_ZONE, "cast('2020-12-30T01:42:00' as timestamp)", LocalDateTime.of(2020, 12, 30, 1, 42, 0),
                        ZonedDateTime.of(2020, 12, 30, 1, 42, 0, 0, DEFAULT_ZONE).toOffsetDateTime()),
                TestParams.passingCase(2214, TIMESTAMP, OBJECT, "cast('2020-12-30T01:42:00' as timestamp)", LocalDateTime.of(2020, 12, 30, 1, 42, 0),
                        LocalDateTime.of(2020, 12, 30, 1, 42)),
                TestParams.failingCase(2215, TIMESTAMP, JSON, "cast('2020-12-30T01:42:00' as timestamp)", LocalDateTime.of(2020, 12, 30, 1, 42, 0))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type JSON from source field '.+' of type TIMESTAMP")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type JSON from source field '.+' of type TIMESTAMP")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of JSON type, but TIMESTAMP was found"),

                // TIMESTAMP WITH TIME ZONE
                TestParams.failingCase(2301, TIMESTAMP_WITH_TIME_ZONE, VARCHAR, "cast('2020-12-30T01:42:00-05:00' as timestamp with time zone)", OffsetDateTime.of(2020, 12, 30, 1, 42, 0, 0, ZoneOffset.ofHours(-5)))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type VARCHAR from source field '.+' of type TIMESTAMP WITH TIME ZONE")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type VARCHAR from source field '.+' of type TIMESTAMP WITH TIME ZONE")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of VARCHAR type, but TIMESTAMP WITH TIME ZONE was found"),
                TestParams.failingCase(2302, TIMESTAMP_WITH_TIME_ZONE, BOOLEAN, "cast('2020-12-30T01:42:00-05:00' as timestamp with time zone)", OffsetDateTime.of(2020, 12, 30, 1, 42, 0, 0, ZoneOffset.ofHours(-5)))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type BOOLEAN from source field '.+' of type TIMESTAMP WITH TIME ZONE")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type BOOLEAN from source field '.+' of type TIMESTAMP WITH TIME ZONE")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of BOOLEAN type, but TIMESTAMP WITH TIME ZONE was found"),
                TestParams.failingCase(2303, TIMESTAMP_WITH_TIME_ZONE, TINYINT, "cast('2020-12-30T01:42:00-05:00' as timestamp with time zone)", OffsetDateTime.of(2020, 12, 30, 1, 42, 0, 0, ZoneOffset.ofHours(-5)))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type TINYINT from source field '.+' of type TIMESTAMP WITH TIME ZONE")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TINYINT from source field '.+' of type TIMESTAMP WITH TIME ZONE")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TINYINT type, but TIMESTAMP WITH TIME ZONE was found"),
                TestParams.failingCase(2304, TIMESTAMP_WITH_TIME_ZONE, SMALLINT, "cast('2020-12-30T01:42:00-05:00' as timestamp with time zone)", OffsetDateTime.of(2020, 12, 30, 1, 42, 0, 0, ZoneOffset.ofHours(-5)))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type SMALLINT from source field '.+' of type TIMESTAMP WITH TIME ZONE")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type SMALLINT from source field '.+' of type TIMESTAMP WITH TIME ZONE")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of SMALLINT type, but TIMESTAMP WITH TIME ZONE was found"),
                TestParams.failingCase(2305, TIMESTAMP_WITH_TIME_ZONE, INTEGER, "cast('2020-12-30T01:42:00-05:00' as timestamp with time zone)", OffsetDateTime.of(2020, 12, 30, 1, 42, 0, 0, ZoneOffset.ofHours(-5)))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type INTEGER from source field '.+' of type TIMESTAMP WITH TIME ZONE")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type INTEGER from source field '.+' of type TIMESTAMP WITH TIME ZONE")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of INTEGER type, but TIMESTAMP WITH TIME ZONE was found"),
                TestParams.failingCase(2306, TIMESTAMP_WITH_TIME_ZONE, BIGINT, "cast('2020-12-30T01:42:00-05:00' as timestamp with time zone)", OffsetDateTime.of(2020, 12, 30, 1, 42, 0, 0, ZoneOffset.ofHours(-5)))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type BIGINT from source field '.+' of type TIMESTAMP WITH TIME ZONE")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type BIGINT from source field '.+' of type TIMESTAMP WITH TIME ZONE")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of BIGINT type, but TIMESTAMP WITH TIME ZONE was found"),
                TestParams.failingCase(2307, TIMESTAMP_WITH_TIME_ZONE, DECIMAL, "cast('2020-12-30T01:42:00-05:00' as timestamp with time zone)", OffsetDateTime.of(2020, 12, 30, 1, 42, 0, 0, ZoneOffset.ofHours(-5)))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type DECIMAL\\(76, 38\\) from source field '.+' of type TIMESTAMP WITH TIME ZONE")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type DECIMAL\\(76, 38\\) from source field '.+' of type TIMESTAMP WITH TIME ZONE")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of DECIMAL type, but TIMESTAMP WITH TIME ZONE was found"),
                TestParams.failingCase(2308, TIMESTAMP_WITH_TIME_ZONE, REAL, "cast('2020-12-30T01:42:00-05:00' as timestamp with time zone)", OffsetDateTime.of(2020, 12, 30, 1, 42, 0, 0, ZoneOffset.ofHours(-5)))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type REAL from source field '.+' of type TIMESTAMP WITH TIME ZONE")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type REAL from source field '.+' of type TIMESTAMP WITH TIME ZONE")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of REAL type, but TIMESTAMP WITH TIME ZONE was found"),
                TestParams.failingCase(2309, TIMESTAMP_WITH_TIME_ZONE, DOUBLE, "cast('2020-12-30T01:42:00-05:00' as timestamp with time zone)", OffsetDateTime.of(2020, 12, 30, 1, 42, 0, 0, ZoneOffset.ofHours(-5)))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type DOUBLE from source field '.+' of type TIMESTAMP WITH TIME ZONE")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type DOUBLE from source field '.+' of type TIMESTAMP WITH TIME ZONE")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of DOUBLE type, but TIMESTAMP WITH TIME ZONE was found"),
                TestParams.passingCase(2310, TIMESTAMP_WITH_TIME_ZONE, TIME, "cast('2020-12-30T01:42:00-05:00' as timestamp with time zone)", OffsetDateTime.of(2020, 12, 30, 1, 42, 0, 0, ZoneOffset.ofHours(-5)),
                        LocalTime.of(1, 42))
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TIME type, but TIMESTAMP WITH TIME ZONE was found"),
                TestParams.passingCase(2311, TIMESTAMP_WITH_TIME_ZONE, DATE, "cast('2020-12-30T01:42:00-05:00' as timestamp with time zone)", OffsetDateTime.of(2020, 12, 30, 1, 42, 0, 0, ZoneOffset.ofHours(-5)),
                        LocalDate.of(2020, 12, 30))
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of DATE type, but TIMESTAMP WITH TIME ZONE was found"),
                TestParams.passingCase(2312, TIMESTAMP_WITH_TIME_ZONE, TIMESTAMP, "cast('2020-12-30T01:42:00-05:00' as timestamp with time zone)", OffsetDateTime.of(2020, 12, 30, 1, 42, 0, 0, ZoneOffset.ofHours(-5)),
                        LocalDateTime.of(2020, 12, 30, 1, 42))
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TIMESTAMP type, but TIMESTAMP WITH TIME ZONE was found"),
                TestParams.passingCase(2313, TIMESTAMP_WITH_TIME_ZONE, TIMESTAMP_WITH_TIME_ZONE, "cast('2020-12-30T01:42:00-05:00' as timestamp with time zone)", OffsetDateTime.of(2020, 12, 30, 1, 42, 0, 0, ZoneOffset.ofHours(-5)),
                        OffsetDateTime.of(2020, 12, 30, 1, 42, 0, 0, ZoneOffset.ofHours(-5))),
                TestParams.passingCase(2314, TIMESTAMP_WITH_TIME_ZONE, OBJECT, "cast('2020-12-30T01:42:00-05:00' as timestamp with time zone)", OffsetDateTime.of(2020, 12, 30, 1, 42, 0, 0, ZoneOffset.ofHours(-5)),
                        OffsetDateTime.of(2020, 12, 30, 1, 42, 0, 0, ZoneOffset.ofHours(-5))),
                TestParams.failingCase(2315, TIMESTAMP_WITH_TIME_ZONE, JSON, "cast('2020-12-30T01:42:00-05:00' as timestamp with time zone)", OffsetDateTime.of(2020, 12, 30, 1, 42, 0, 0, ZoneOffset.ofHours(-5)))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type JSON from source field '.+' of type TIMESTAMP WITH TIME ZONE")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type JSON from source field '.+' of type TIMESTAMP WITH TIME ZONE")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of JSON type, but TIMESTAMP WITH TIME ZONE was found"),

                // OBJECT
                TestParams.failingCase(2401, OBJECT, VARCHAR, "cast('foo' as object)", new Value(42))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type VARCHAR from source field 'EXPR\\$\\d' of type OBJECT")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type VARCHAR from source field 'EXPR\\$\\d' of type OBJECT")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of VARCHAR type, but OBJECT was found"),
                TestParams.failingCase(2402, OBJECT, BOOLEAN, "cast(true as object)", new Value(42))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type BOOLEAN from source field 'EXPR\\$\\d' of type OBJECT")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type BOOLEAN from source field 'EXPR\\$\\d' of type OBJECT")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of BOOLEAN type, but OBJECT was found"),
                TestParams.failingCase(2403, OBJECT, TINYINT, "cast(42 as object)", new Value(42))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type TINYINT from source field 'EXPR\\$\\d' of type OBJECT")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TINYINT from source field 'EXPR\\$\\d' of type OBJECT")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TINYINT type, but OBJECT was found"),
                TestParams.failingCase(2404, OBJECT, SMALLINT, "cast(420 as object)", new Value(42))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type SMALLINT from source field 'EXPR\\$\\d' of type OBJECT")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type SMALLINT from source field 'EXPR\\$\\d' of type OBJECT")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of SMALLINT type, but OBJECT was found"),
                TestParams.failingCase(2405, OBJECT, INTEGER, "cast(420000 as object)", new Value(42))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type INTEGER from source field 'EXPR\\$\\d' of type OBJECT")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type INTEGER from source field 'EXPR\\$\\d' of type OBJECT")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of INTEGER type, but OBJECT was found"),
                TestParams.failingCase(2406, OBJECT, BIGINT, "cast(4200000000 as object)", new Value(42))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type BIGINT from source field 'EXPR\\$\\d' of type OBJECT")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type BIGINT from source field 'EXPR\\$\\d' of type OBJECT")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of BIGINT type, but OBJECT was found"),
                TestParams.failingCase(2407, OBJECT, DECIMAL, "cast(cast(1.5 as decimal) as object)", new Value(42))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type DECIMAL\\(76, 38\\) from source field 'EXPR\\$\\d' of type OBJECT")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type DECIMAL\\(76, 38\\) from source field 'EXPR\\$\\d' of type OBJECT")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of DECIMAL type, but OBJECT was found"),
                TestParams.failingCase(2408, OBJECT, REAL, "cast(cast(1.5 as real) as object)", new Value(42))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type REAL from source field 'EXPR\\$\\d' of type OBJECT")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type REAL from source field 'EXPR\\$\\d' of type OBJECT")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of REAL type, but OBJECT was found"),
                TestParams.failingCase(2409, OBJECT, DOUBLE, "cast(cast(1.5 as double) as object)", new Value(42))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type DOUBLE from source field 'EXPR\\$\\d' of type OBJECT")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type DOUBLE from source field 'EXPR\\$\\d' of type OBJECT")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of DOUBLE type, but OBJECT was found"),
                TestParams.failingCase(2410, OBJECT, TIME, "cast(cast('01:42:00' as time) as object)", new Value(42))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type TIME from source field 'EXPR\\$\\d' of type OBJECT")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TIME from source field 'EXPR\\$\\d' of type OBJECT")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TIME type, but OBJECT was found"),
                TestParams.failingCase(2411, OBJECT, DATE, "cast(cast('2020-12-30' as date) as object)", new Value(42))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type DATE from source field 'EXPR\\$\\d' of type OBJECT")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type DATE from source field 'EXPR\\$\\d' of type OBJECT")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of DATE type, but OBJECT was found"),
                TestParams.failingCase(2412, OBJECT, TIMESTAMP, "cast(cast('2020-12-30T01:42:00' as timestamp) as object)", new Value(42))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type TIMESTAMP from source field 'EXPR\\$\\d' of type OBJECT")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TIMESTAMP from source field 'EXPR\\$\\d' of type OBJECT")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TIMESTAMP type, but OBJECT was found"),
                TestParams.failingCase(2413, OBJECT, TIMESTAMP_WITH_TIME_ZONE, "cast(cast('2020-12-30T01:42:00-05:00' as timestamp with time zone) as object)", new Value(42))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type TIMESTAMP WITH TIME ZONE from source field 'EXPR\\$\\d' of type OBJECT")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TIMESTAMP WITH TIME ZONE from source field 'EXPR\\$\\d' of type OBJECT")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TIMESTAMP WITH TIME ZONE type, but OBJECT was found"),
                TestParams.passingCase(2414, OBJECT, OBJECT, "cast('foo' as object)", "foo", "foo"),
                TestParams.failingCase(2415, OBJECT, JSON, "cast(cast('2020-12-30T01:42:00-05:00' as timestamp with time zone) as object)", new Value(42))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type JSON from source field 'EXPR\\$\\d' of type OBJECT")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type JSON from source field 'EXPR\\$\\d' of type OBJECT")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of JSON type, but OBJECT was found"),

                // JSON
                TestParams.failingCase(2501, JSON, VARCHAR, "CAST('\"foo\"' AS JSON)", new HazelcastJsonValue("\"foo\""))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type VARCHAR from source field '.+' of type JSON")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type VARCHAR from source field '.+' of type JSON")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of VARCHAR type, but JSON was found"),
                TestParams.failingCase(2502, JSON, BOOLEAN, "CAST('true' AS JSON)", new HazelcastJsonValue("true"))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type BOOLEAN from source field '.+' of type JSON")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type BOOLEAN from source field '.+' of type JSON")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of BOOLEAN type, but JSON was found"),
                TestParams.failingCase(2503, JSON, TINYINT, "CAST('42' AS JSON)", new HazelcastJsonValue("42"))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type TINYINT from source field '.+' of type JSON")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TINYINT from source field '.+' of type JSON")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TINYINT type, but JSON was found"),
                TestParams.failingCase(2506, JSON, SMALLINT, "CAST('42' AS JSON)", new HazelcastJsonValue("42"))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type SMALLINT from source field '.+' of type JSON")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type SMALLINT from source field '.+' of type JSON")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of SMALLINT type, but JSON was found"),
                TestParams.failingCase(2509, JSON, INTEGER, "CAST('42' AS JSON)", new HazelcastJsonValue("42"))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type INTEGER from source field '.+' of type JSON")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type INTEGER from source field '.+' of type JSON")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of INTEGER type, but JSON was found"),
                TestParams.failingCase(2512, JSON, BIGINT, "CAST('42' AS JSON)", new HazelcastJsonValue("42"))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type BIGINT from source field '.+' of type JSON")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type BIGINT from source field '.+' of type JSON")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of BIGINT type, but JSON was found"),
                TestParams.failingCase(2515, JSON, DECIMAL, "CAST('1.5' AS JSON)", new HazelcastJsonValue("1.5"))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type DECIMAL\\(76, 38\\) from source field '.+' of type JSON")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type DECIMAL\\(76, 38\\) from source field '.+' of type JSON")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of DECIMAL type, but JSON was found"),
                TestParams.failingCase(2517, JSON, REAL, "CAST('1.5' AS JSON)", new HazelcastJsonValue("1.5"))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type REAL from source field '.+' of type JSON")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type REAL from source field '.+' of type JSON")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of REAL type, but JSON was found"),
                TestParams.failingCase(2519, JSON, DOUBLE, "CAST('1.5' AS JSON)", new HazelcastJsonValue("1.5"))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type DOUBLE from source field '.+' of type JSON")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type DOUBLE from source field '.+' of type JSON")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of DOUBLE type, but JSON was found"),
                TestParams.failingCase(2521, JSON, TIME, "CAST('\"01:42:01\"' AS JSON)", new HazelcastJsonValue("\"01:42:01\""))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type TIME from source field '.+' of type JSON")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TIME from source field '.+' of type JSON")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TIME type, but JSON was found"),
                TestParams.failingCase(2523, JSON, DATE, "CAST('\"2020-12-30\"' AS JSON)", new HazelcastJsonValue("\"2020-12-30\""))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type DATE from source field '.+' of type JSON")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type DATE from source field '.+' of type JSON")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of DATE type, but JSON was found"),
                TestParams.failingCase(2525, JSON, TIMESTAMP, "CAST('\"2020-12-30T01:42:00\"' AS JSON)", new HazelcastJsonValue("\"2020-12-30T01:42:00\""))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type TIMESTAMP from source field '.+' of type JSON")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TIMESTAMP from source field '.+' of type JSON")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TIMESTAMP type, but JSON was found"),
                TestParams.failingCase(2527, JSON, TIMESTAMP_WITH_TIME_ZONE, "CAST('\"2020-12-30T01:42:00-05:00\"' AS JSON)", new HazelcastJsonValue("\"2020-12-30T01:42:00-05:00\""))
                        .withExpectedLiteralFailureRegex("Cannot assign to target field 'field1' of type TIMESTAMP WITH TIME ZONE from source field '.+' of type JSON")
                        .withExpectedColumnFailureRegex("Cannot assign to target field 'field1' of type TIMESTAMP WITH TIME ZONE from source field '.+' of type JSON")
                        .withExpectedDynamicParameterFailureRegex("Parameter at position 0 must be of TIMESTAMP WITH TIME ZONE type, but JSON was found"),
                TestParams.passingCase(2529, JSON, OBJECT, "CAST('\"foo\"' AS JSON)", new HazelcastJsonValue("\"foo\""), new HazelcastJsonValue("\"foo\"")),
                TestParams.passingCase(2530, JSON, JSON, "CAST('{\"k\":\"v\"}' AS JSON)", new HazelcastJsonValue("{\"k\":\"v\"}"), new HazelcastJsonValue("{\"k\":\"v\"}")),
        };
    }

    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
    }

    @Test
    public void test_insertValues() {
        String targetClassName = ExpressionValue.classForType(testParams.targetType);
        execute("CREATE MAPPING m type IMap " +
                "OPTIONS(" +
                "'keyFormat'='int', " +
                "'valueFormat'='java', " +
                "'valueJavaClass'='" + targetClassName +
                "')"
        );

        try {
            execute("SINK INTO m VALUES(0, " + testParams.srcLiteral + ", 0)");
            throwIfFailureWasExpected(testParams.expectedLiteralFailureRegex);
            assertThat(extractValue("m", "field1")).isIn(testParams.expectedTargetValues);
        } catch (Exception e) {
            assertExceptionMatches(e, testParams.expectedLiteralFailureRegex);
        }
    }

    @Test
    public void test_insertSelect() {
        // the TestBatchSource doesn't support OBJECT/NULL type
        assumeFalse(testParams.srcType == OBJECT || testParams.srcType == NULL);

        // HazelcastJsonValue isn't java-serializable, it doesn't work with TestBatchSqlConnector
        // TODO we might replace test-batch-source with an imap which doesn't have this issue
        assumeFalse(testParams.srcValue instanceof HazelcastJsonValue);

        String targetClassName = ExpressionValue.classForType(testParams.targetType);
        TestBatchSqlConnector.create(sqlService, "src", singletonList("v"),
                singletonList(testParams.srcType),
                singletonList(new String[]{String.valueOf(testParams.srcValue)}));
        execute("CREATE MAPPING target TYPE IMap " +
                "OPTIONS(" +
                "'keyFormat'='int', " +
                "'valueFormat'='java', " +
                "'valueJavaClass'='" + targetClassName +
                "')"
        );

        try {
            execute("SINK INTO target SELECT 0, v, 0 FROM src");
            throwIfFailureWasExpected(testParams.expectedColumnFailureRegex);
            assertThat(extractValue("target", "field1")).isIn(testParams.expectedTargetValues);
        } catch (Exception e) {
            assertExceptionMatches(e, testParams.expectedColumnFailureRegex);
        }
    }

    @Test
    public void test_insertSelect_withLiteral() {
        String targetClassName = ExpressionValue.classForType(testParams.targetType);
        TestBatchSqlConnector.create(sqlService, "src", 1);
        execute("CREATE MAPPING target TYPE IMap " +
                "OPTIONS(" +
                "'keyFormat'='int', " +
                "'valueFormat'='java', " +
                "'valueJavaClass'='" + targetClassName +
                "')"
        );

        try {
            execute("SINK INTO target SELECT 0, " + testParams.srcLiteral + ", 0 FROM src");
            throwIfFailureWasExpected(testParams.expectedLiteralFailureRegex);
            assertThat(extractValue("target", "field1")).isIn(testParams.expectedTargetValues);
        } catch (Exception e) {
            assertExceptionMatches(e, testParams.expectedLiteralFailureRegex);
        }
    }

    @Test
    public void test_insertDynamicParameters() {
        String targetClassName = ExpressionValue.classForType(testParams.targetType);
        execute("CREATE MAPPING m type IMap " +
                "OPTIONS(" +
                "'keyFormat'='int', " +
                "'valueFormat'='java', " +
                "'valueJavaClass'='" + targetClassName +
                "')"
        );

        try {
            execute("SINK INTO m VALUES(0, ?, 0)", testParams.srcValue);
            throwIfFailureWasExpected(testParams.expectedDynamicParameterFailureRegex);
            assertThat(extractValue("m", "field1")).isIn(testParams.expectedTargetValues);
        } catch (Exception e) {
            assertExceptionMatches(e, testParams.expectedDynamicParameterFailureRegex);
        }
    }

    @Test
    public void test_update_literals() {
        String targetClassName = ExpressionValue.classForType(testParams.targetType);
        execute("CREATE MAPPING m type IMap " +
                "OPTIONS (" +
                "'keyFormat'='int', " +
                "'valueFormat'='java', " +
                "'valueJavaClass'='" + targetClassName +
                "')"
        );
        instance().getMap("m").put(0, ExpressionValue.create(targetClassName));

        try {
            execute("UPDATE m SET field1 = " + testParams.srcLiteral);
            throwIfFailureWasExpected(testParams.expectedLiteralFailureRegex);
            assertThat(extractValue("m", "field1")).isIn(testParams.expectedTargetValues);
        } catch (Exception e) {
            assertExceptionMatches(e, testParams.expectedLiteralFailureRegex);
        }
    }

    @Test
    public void test_update_columns() {
        assumeFalse(testParams.srcType == NULL);

        Class<? extends ExpressionBiValue> valueClass = ExpressionBiValue.biClassForType(testParams.targetType, testParams.srcType);
        execute("CREATE MAPPING m TYPE IMap " +
                "OPTIONS(" +
                "'keyFormat'='int', " +
                "'valueFormat'='java', " +
                "'valueJavaClass'='" + valueClass.getName() +
                "')"
        );
        instance().getMap("m").put(0, ExpressionBiValue.createBiValue(valueClass, null, testParams.srcValue));

        try {
            execute("UPDATE m SET field1 = field2");
            throwIfFailureWasExpected(testParams.expectedColumnFailureRegex);
            assertThat(extractValue("m", "field1")).isIn(testParams.expectedTargetValues);
        } catch (Exception e) {
            assertExceptionMatches(e, testParams.expectedColumnFailureRegex);
        }
    }

    @Test
    public void test_update_dynamicParameters() {
        String targetClassName = ExpressionValue.classForType(testParams.targetType);
        execute("CREATE MAPPING m type IMap " +
                "OPTIONS (" +
                "'keyFormat'='int', " +
                "'valueFormat'='java', " +
                "'valueJavaClass'='" + targetClassName +
                "')"
        );
        instance().getMap("m").put(0, ExpressionValue.create(targetClassName));

        try {
            execute("UPDATE m SET field1 = ?", testParams.srcValue);
            throwIfFailureWasExpected(testParams.expectedDynamicParameterFailureRegex);
            assertThat(extractValue("m", "field1")).isIn(testParams.expectedTargetValues);
        } catch (Exception e) {
            assertExceptionMatches(e, testParams.expectedDynamicParameterFailureRegex);
        }
    }

    private void execute(String sql, Object... arguments) {
        logger.info(sql);
        //noinspection EmptyTryBlock
        try (SqlResult ignored = sqlService.execute(sql, arguments)) {
        }
    }

    private static void throwIfFailureWasExpected(Pattern pattern) {
        if (pattern != null) {
            fail("Expected to fail with \"" + pattern + "\", but no exception was thrown");
        }
    }

    private static void assertExceptionMatches(Exception e, Pattern pattern) {
        if (pattern == null) {
            throw new AssertionError("The query failed unexpectedly: " + e, e);
        }

        if (!pattern.matcher(e.getMessage()).find()) {
            throw new AssertionError("\n'" + e.getMessage() + "'\ndidn't contain \n'" + pattern + "'", e);
        }
    }

    @SuppressWarnings("SameParameterValue")
    private static Object extractValue(String mapName, String fieldName) throws Exception {
        Object valueWrapper = instance().getMap(mapName).get(0);
        return valueWrapper.getClass().getField(fieldName).get(valueWrapper);
    }

    private static final class TestParams {
        private final int testId;
        private final QueryDataTypeFamily srcType;
        private final QueryDataTypeFamily targetType;
        private final String srcLiteral;
        private final Object srcValue;
        private final List<Object> expectedTargetValues;
        private Pattern expectedLiteralFailureRegex;
        private Pattern expectedColumnFailureRegex;
        private Pattern expectedDynamicParameterFailureRegex;

        private TestParams(int testId, QueryDataTypeFamily srcType, QueryDataTypeFamily targetType, String srcLiteral,
                           Object srcValue, List<Object> expectedTargetValues) {
            this.testId = testId;
            this.srcType = srcType;
            this.targetType = targetType;
            this.srcLiteral = srcLiteral;
            this.srcValue = srcValue;
            this.expectedTargetValues = expectedTargetValues;
        }

        static TestParams passingCase(int testId, QueryDataTypeFamily srcType, QueryDataTypeFamily targetType,
                                      String valueLiteral, Object value, Object... targetValues) {
            return new TestParams(testId, srcType, targetType, valueLiteral, value, asList(targetValues));
        }

        static TestParams failingCase(int testId, QueryDataTypeFamily srcType, QueryDataTypeFamily targetType,
                                      String valueLiteral, Object value) {
            return new TestParams(testId, srcType, targetType, valueLiteral, value, null);
        }

        /**
         * This test case is expected to fail if the source is a literal (the
         * {@link #test_insertValues()} method).
         */
        private TestParams withExpectedLiteralFailureRegex(String failureRegex) {
            this.expectedLiteralFailureRegex = Pattern.compile(failureRegex);
            return this;
        }

        /**
         * This test case is expected to fail if the source is a column (the
         * {@link #test_insertSelect()} method).
         */
        private TestParams withExpectedColumnFailureRegex(String failureRegex) {
            this.expectedColumnFailureRegex = Pattern.compile(failureRegex);
            return this;
        }

        /**
         * This test case is expected to fail if the source is a dynamic parameter (the
         * {@link #test_insertDynamicParameters()} method).
         */
        private TestParams withExpectedDynamicParameterFailureRegex(String failureRegex) {
            this.expectedDynamicParameterFailureRegex = Pattern.compile(failureRegex);
            return this;
        }

        /**
         * This test case is expected to fail if the source is any of literal, column
         * or dynamic parameter.
         */
        private TestParams withExpectedFailureRegex(String failureRegex) {
            this.expectedLiteralFailureRegex = this.expectedColumnFailureRegex = this.expectedDynamicParameterFailureRegex =
                    Pattern.compile(failureRegex);
            return this;
        }

        @Override
        public String toString() {
            return "TestParams{" +
                    "id=" + testId +
                    ", srcType=" + srcType +
                    ", targetType=" + targetType +
                    ", srcValue=" + srcValue +
                    '}';
        }
    }

    private static class Value implements Serializable {

        public int value;

        private Value(int value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Value value1 = (Value) o;
            return value == value1.value;
        }

        @Override
        public int hashCode() {
            return Objects.hash(value);
        }
    }
}
