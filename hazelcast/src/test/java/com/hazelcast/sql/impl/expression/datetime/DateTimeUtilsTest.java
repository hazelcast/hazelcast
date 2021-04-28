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

package com.hazelcast.sql.impl.expression.datetime;

import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.junit.Assert.assertEquals;

@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DateTimeUtilsTest {

    @Test
    public void test_parse_timestampTZ() {
        OffsetDateTime date = OffsetDateTime.of(2010, 10, 20, 10, 20, 30, 0, ZoneOffset.UTC);
        assertEquals(date, DateTimeUtils.parseAsOffsetDateTime("2010-10-20 10:20:30+00:00"));
        assertEquals(date, DateTimeUtils.parseAsOffsetDateTime("2010-10-20 10:20:30+00"));
        assertEquals(date, DateTimeUtils.parseAsOffsetDateTime("2010-10-20 10:20:30"));

        date = OffsetDateTime.of(2010, 10, 20, 10, 20, 30, 0, ZoneOffset.of("+2"));
        assertEquals(date, DateTimeUtils.parseAsOffsetDateTime("2010-10-20 10:20:30+02"));
        assertEquals(date, DateTimeUtils.parseAsOffsetDateTime("2010-10-20 10:20:30+02:00"));

        date = OffsetDateTime.of(2010, 10, 20, 10, 20, 0, 0, ZoneOffset.of("+2"));
        assertEquals(date, DateTimeUtils.parseAsOffsetDateTime("2010-10-20 10:20:00+02:00"));
        assertEquals(date, DateTimeUtils.parseAsOffsetDateTime("2010-10-20 10:20:00+02"));
        assertEquals(date, DateTimeUtils.parseAsOffsetDateTime("2010-10-20 10:20+02"));
        assertEquals(date, DateTimeUtils.parseAsOffsetDateTime("2010-10-20 10:20+2"));
        assertEquals(date, DateTimeUtils.parseAsOffsetDateTime("2010-10-20 10:20+02:00"));


        date = OffsetDateTime.of(2010, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        assertEquals(date, DateTimeUtils.parseAsOffsetDateTime("2010-01-01"));
        assertEquals(date, DateTimeUtils.parseAsOffsetDateTime("2010-01-01 00:00:00"));
        assertEquals(date, DateTimeUtils.parseAsOffsetDateTime("2010-1-1 +00"));
        assertEquals(date, DateTimeUtils.parseAsOffsetDateTime("2010-1-1 +0"));
        assertEquals(date, DateTimeUtils.parseAsOffsetDateTime("2010-1-1+0"));
        assertEquals(date, DateTimeUtils.parseAsOffsetDateTime("2010-1-1+00"));
        assertEquals(date, DateTimeUtils.parseAsOffsetDateTime("2010-1-1+0:00"));
        assertEquals(date, DateTimeUtils.parseAsOffsetDateTime("2010-1-1 +00:00"));
    }

    @Test
    public void test_timestamp() {
        LocalDateTime date = LocalDateTime.of(2010, 10, 20, 10, 20, 30, 0);
        assertEquals(date, DateTimeUtils.parseAsLocalDateTime("2010-10-20 10:20:30"));


        date = LocalDateTime.of(2010, 10, 20, 10, 20, 0, 0);
        assertEquals(date, DateTimeUtils.parseAsLocalDateTime("2010-10-20 10:20:00"));
        assertEquals(date, DateTimeUtils.parseAsLocalDateTime("2010-10-20 10:20"));
        assertEquals(date, DateTimeUtils.parseAsLocalDateTime("2010-10-20 10:20"));
        assertEquals(date, DateTimeUtils.parseAsLocalDateTime("2010-10-20 10:20"));


        date = LocalDateTime.of(2010, 1, 1, 0, 0, 0, 0);
        assertEquals(date, DateTimeUtils.parseAsLocalDateTime("2010-01-01"));
        assertEquals(date, DateTimeUtils.parseAsLocalDateTime("2010-01-01 00:00:00"));
        assertEquals(date, DateTimeUtils.parseAsLocalDateTime("2010-1-1"));
        assertEquals(date, DateTimeUtils.parseAsLocalDateTime("2010-1-1 00:00"));
        assertEquals(date, DateTimeUtils.parseAsLocalDateTime("2010-1-1 00:00:00"));
        assertEquals(date, DateTimeUtils.parseAsLocalDateTime("2010-1-1 00:00:00.0000"));
    }
}
