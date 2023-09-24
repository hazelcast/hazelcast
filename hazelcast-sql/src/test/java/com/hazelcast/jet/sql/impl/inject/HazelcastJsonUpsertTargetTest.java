/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.inject;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.util.JavaVersion;
import com.hazelcast.sql.impl.type.QueryDataType;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.List;

import static com.hazelcast.internal.util.JavaVersion.JAVA_19;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(JUnitParamsRunner.class)
public class HazelcastJsonUpsertTargetTest extends UpsertTargetTestSupport {

    @Test
    public void test_set() {
        UpsertTarget target = new HazelcastJsonUpsertTarget();
        UpsertConverter converter = target.createConverter(List.of(
                field("null", QueryDataType.OBJECT),
                field("object", QueryDataType.OBJECT),
                field("string", QueryDataType.VARCHAR),
                field("boolean", QueryDataType.BOOLEAN),
                field("byte", QueryDataType.TINYINT),
                field("short", QueryDataType.SMALLINT),
                field("int", QueryDataType.INT),
                field("long", QueryDataType.BIGINT),
                field("float", QueryDataType.REAL),
                field("double", QueryDataType.DOUBLE),
                field("decimal", QueryDataType.DECIMAL),
                field("time", QueryDataType.TIME),
                field("date", QueryDataType.DATE),
                field("timestamp", QueryDataType.TIMESTAMP),
                field("timestampTz", QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME)
        ));

        Object hazelcastJson = converter.applyRow(
                null,
                new JsonObject(),
                "string",
                true,
                (byte) 127,
                (short) 32767,
                2147483647,
                9223372036854775807L,
                1234567890.1F,
                123451234567890.1D,
                new BigDecimal("9223372036854775.123"),
                LocalTime.of(12, 23, 34),
                LocalDate.of(2020, 9, 9),
                LocalDateTime.of(2020, 9, 9, 12, 23, 34, 100_000_000),
                OffsetDateTime.of(2020, 9, 9, 12, 23, 34, 200_000_000, UTC)
        );

        String expectedJson;
        if (JavaVersion.isAtLeast(JAVA_19)) {
            // old, old, old JDK bug fixed now https://bugs.openjdk.org/browse/JDK-4511638
            expectedJson = "{"
                    + "\"null\":null"
                    + ",\"object\":{}"
                    + ",\"string\":\"string\""
                    + ",\"boolean\":true"
                    + ",\"byte\":127"
                    + ",\"short\":32767"
                    + ",\"int\":2147483647"
                    + ",\"long\":9223372036854775807"
                    + ",\"float\":1.234568E9"
                    + ",\"double\":1.234512345678901E14"
                    + ",\"decimal\":\"9223372036854775.123\""
                    + ",\"time\":\"12:23:34\""
                    + ",\"date\":\"2020-09-09\""
                    + ",\"timestamp\":\"2020-09-09T12:23:34.100\""
                    + ",\"timestampTz\":\"2020-09-09T12:23:34.200Z\""
                    + "}";
        } else {
            expectedJson = "{"
                    + "\"null\":null"
                    + ",\"object\":{}"
                    + ",\"string\":\"string\""
                    + ",\"boolean\":true"
                    + ",\"byte\":127"
                    + ",\"short\":32767"
                    + ",\"int\":2147483647"
                    + ",\"long\":9223372036854775807"
                    + ",\"float\":1.23456794E9"
                    + ",\"double\":1.234512345678901E14"
                    + ",\"decimal\":\"9223372036854775.123\""
                    + ",\"time\":\"12:23:34\""
                    + ",\"date\":\"2020-09-09\""
                    + ",\"timestamp\":\"2020-09-09T12:23:34.100\""
                    + ",\"timestampTz\":\"2020-09-09T12:23:34.200Z\""
                    + "}";
        }
        assertThat(hazelcastJson).isEqualTo(new HazelcastJsonValue(expectedJson));
    }

    @SuppressWarnings("unused")
    private Object[] values() {
        String expectedFloat = JavaVersion.isAtLeast(JAVA_19) ? "1.234568E9" : "1.23456794E9";
        return new Object[]{
                new Object[]{null, "null"},
                new Object[]{new JsonObject(), "{}"},
                new Object[]{"string", "\"string\""},
                new Object[]{true, "true"},
                new Object[]{(byte) 127, "127"},
                new Object[]{(short) 32767, "32767"},
                new Object[]{2147483647, "2147483647"},
                new Object[]{9223372036854775807L, "9223372036854775807"},
                new Object[]{1234567890.1F, expectedFloat},
                new Object[]{123451234567890.1D, "1.234512345678901E14"},
                new Object[]{new BigDecimal("9223372036854775.123"), "\"9223372036854775.123\""},
                new Object[]{LocalTime.of(12, 23, 34), "\"12:23:34\""},
                new Object[]{LocalDate.of(2020, 9, 9), "\"2020-09-09\""},
                new Object[]{LocalDateTime.of(2020, 9, 9, 12, 23, 34, 100_000_000), "\"2020-09-09T12:23:34.100\""},
                new Object[]{OffsetDateTime.of(2020, 9, 9, 12, 23, 34, 200_000_000, UTC), "\"2020-09-09T12:23:34.200Z\""},
        };
    }

    @Test
    @Parameters(method = "values")
    public void when_typeIsObject_then_allValuesAreAllowed(Object value, String expected) {
        UpsertTarget target = new HazelcastJsonUpsertTarget();
        UpsertConverter converter = target.createConverter(List.of(
                field("object", QueryDataType.OBJECT)
        ));

        Object hazelcastJson = converter.applyRow(value);

        assertThat(hazelcastJson).isEqualTo(new HazelcastJsonValue("{\"object\":" + expected + "}"));
    }
}
