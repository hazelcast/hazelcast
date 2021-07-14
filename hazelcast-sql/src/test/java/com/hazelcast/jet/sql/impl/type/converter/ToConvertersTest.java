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

package com.hazelcast.jet.sql.impl.type.converter;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Date;
import java.util.GregorianCalendar;

import static com.hazelcast.jet.sql.impl.type.converter.ToConverters.getToConverter;
import static com.hazelcast.sql.impl.type.QueryDataType.DECIMAL_BIG_INTEGER;
import static com.hazelcast.sql.impl.type.QueryDataType.TIMESTAMP_WITH_TZ_CALENDAR;
import static com.hazelcast.sql.impl.type.QueryDataType.TIMESTAMP_WITH_TZ_DATE;
import static com.hazelcast.sql.impl.type.QueryDataType.TIMESTAMP_WITH_TZ_INSTANT;
import static com.hazelcast.sql.impl.type.QueryDataType.TIMESTAMP_WITH_TZ_ZONED_DATE_TIME;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR_CHARACTER;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ToConvertersTest {

    @Test
    public void test_charConversion() {
        Object converted = getToConverter(VARCHAR_CHARACTER).convert("a");

        assertThat(converted).isEqualTo('a');
    }

    @Test
    public void test_bigIntegerConversion() {
        Object converted = getToConverter(DECIMAL_BIG_INTEGER).convert(new BigDecimal("1"));

        assertThat(converted).isEqualTo(new BigInteger("1"));
    }

    @Test
    public void test_dateConversion() {
        OffsetDateTime time = OffsetDateTime.of(2020, 9, 8, 11, 4, 0, 123, UTC);

        Object converted = getToConverter(TIMESTAMP_WITH_TZ_DATE).convert(time);

        assertThat(converted).isEqualTo(Date.from(time.toInstant()));
    }

    @Test
    public void test_calendarConversion() {
        OffsetDateTime time = OffsetDateTime.of(2020, 9, 8, 11, 4, 0, 0, UTC);

        Object converted = getToConverter(TIMESTAMP_WITH_TZ_CALENDAR).convert(time);

        assertThat(converted).isEqualTo(GregorianCalendar.from(time.toZonedDateTime()));
    }

    @Test
    public void test_instantConversion() {
        OffsetDateTime time = OffsetDateTime.of(2020, 9, 8, 11, 4, 0, 0, UTC);

        Object converted = getToConverter(TIMESTAMP_WITH_TZ_INSTANT).convert(time);

        assertThat(converted).isEqualTo(Instant.ofEpochMilli(1599563040000L));
    }

    @Test
    public void test_zonedDateTimeConversion() {
        OffsetDateTime time = OffsetDateTime.of(2020, 9, 8, 11, 4, 0, 0, UTC);

        Object converted = getToConverter(TIMESTAMP_WITH_TZ_ZONED_DATE_TIME).convert(time);

        assertThat(converted).isEqualTo(time.toZonedDateTime());
    }
}
