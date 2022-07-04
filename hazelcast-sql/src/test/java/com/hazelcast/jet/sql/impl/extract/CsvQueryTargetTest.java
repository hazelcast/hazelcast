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

package com.hazelcast.jet.sql.impl.extract;

import com.hazelcast.sql.impl.extract.QueryExtractor;
import com.hazelcast.sql.impl.extract.QueryTarget;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

import static com.hazelcast.sql.impl.type.QueryDataType.BIGINT;
import static com.hazelcast.sql.impl.type.QueryDataType.BOOLEAN;
import static com.hazelcast.sql.impl.type.QueryDataType.DATE;
import static com.hazelcast.sql.impl.type.QueryDataType.DECIMAL;
import static com.hazelcast.sql.impl.type.QueryDataType.DOUBLE;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static com.hazelcast.sql.impl.type.QueryDataType.OBJECT;
import static com.hazelcast.sql.impl.type.QueryDataType.REAL;
import static com.hazelcast.sql.impl.type.QueryDataType.SMALLINT;
import static com.hazelcast.sql.impl.type.QueryDataType.TIME;
import static com.hazelcast.sql.impl.type.QueryDataType.TIMESTAMP;
import static com.hazelcast.sql.impl.type.QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME;
import static com.hazelcast.sql.impl.type.QueryDataType.TINYINT;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;
import static java.time.ZoneOffset.UTC;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CsvQueryTargetTest {

    @Test
    public void test_get() {
        QueryTarget target = new CsvQueryTarget(asList(
                "nonExisting",
                "string",
                "boolean",
                "byte",
                "short",
                "int",
                "long",
                "float",
                "double",
                "decimal",
                "time",
                "date",
                "timestamp",
                "timestampTz",
                "object"));
        QueryExtractor topExtractor = target.createExtractor(null, OBJECT);
        QueryExtractor nonExistingExtractor = target.createExtractor("nonExisting", OBJECT);
        QueryExtractor stringExtractor = target.createExtractor("string", VARCHAR);
        QueryExtractor booleanExtractor = target.createExtractor("boolean", BOOLEAN);
        QueryExtractor byteExtractor = target.createExtractor("byte", TINYINT);
        QueryExtractor shortExtractor = target.createExtractor("short", SMALLINT);
        QueryExtractor intExtractor = target.createExtractor("int", INT);
        QueryExtractor longExtractor = target.createExtractor("long", BIGINT);
        QueryExtractor floatExtractor = target.createExtractor("float", REAL);
        QueryExtractor doubleExtractor = target.createExtractor("double", DOUBLE);
        QueryExtractor decimalExtractor = target.createExtractor("decimal", DECIMAL);
        QueryExtractor timeExtractor = target.createExtractor("time", TIME);
        QueryExtractor dateExtractor = target.createExtractor("date", DATE);
        QueryExtractor timestampExtractor = target.createExtractor("timestamp", TIMESTAMP);
        QueryExtractor timestampTzExtractor = target.createExtractor("timestampTz", TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME);
        QueryExtractor objectExtractor = target.createExtractor("object", OBJECT);

        target.setTarget(
                new String[]{
                        null,
                        "string",
                        "true",
                        "127",
                        "32767",
                        "2147483647",
                        "9223372036854775807",
                        "1234567890.1",
                        "123451234567890.1",
                        "9223372036854775.123",
                        "12:23:34",
                        "2020-09-09",
                        "2020-09-09T12:23:34.100",
                        "2020-09-09T12:23:34.200Z",
                        "object"},
                null);

        assertThat(topExtractor.get()).isInstanceOf(String[].class);
        assertThat(nonExistingExtractor.get()).isNull();
        assertThat(stringExtractor.get()).isEqualTo("string");
        assertThat(booleanExtractor.get()).isEqualTo(true);
        assertThat(byteExtractor.get()).isEqualTo((byte) 127);
        assertThat(shortExtractor.get()).isEqualTo((short) 32767);
        assertThat(intExtractor.get()).isEqualTo(2147483647);
        assertThat(longExtractor.get()).isEqualTo(9223372036854775807L);
        assertThat(floatExtractor.get()).isEqualTo(1234567890.1F);
        assertThat(doubleExtractor.get()).isEqualTo(123451234567890.1D);
        assertThat(decimalExtractor.get()).isEqualTo(new BigDecimal("9223372036854775.123"));
        assertThat(timeExtractor.get()).isEqualTo(LocalTime.of(12, 23, 34));
        assertThat(dateExtractor.get()).isEqualTo(LocalDate.of(2020, 9, 9));
        assertThat(timestampExtractor.get()).isEqualTo(LocalDateTime.of(2020, 9, 9, 12, 23, 34, 100_000_000));
        assertThat(timestampTzExtractor.get()).isEqualTo(OffsetDateTime.of(2020, 9, 9, 12, 23, 34, 200_000_000, UTC));
        assertThat(objectExtractor.get()).isNotNull();
    }
}
