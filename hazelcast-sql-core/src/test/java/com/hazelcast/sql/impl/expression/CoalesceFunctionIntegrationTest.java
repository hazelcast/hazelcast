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

package com.hazelcast.sql.impl.expression;

import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.impl.SqlErrorCode;
import org.junit.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

public class CoalesceFunctionIntegrationTest extends ExpressionTestSupport {
    @Test
    public void coalesce() {
        put(1);

        checkValue0("select coalesce(this, 2) from map", SqlColumnType.INTEGER, 1);
        checkValue0("select coalesce(null, this, 2) from map", SqlColumnType.INTEGER, 1);
        checkValue0("select coalesce(null, 2, this) from map", SqlColumnType.INTEGER, 2);
    }

    @Test
    public void fails_whenReturnTypeCantBeInferred() {
        put(1);
        checkFailure0("select coalesce(null) from map", SqlErrorCode.PARSING, "Cannot apply 'COALESCE' function to [UNKNOWN] (consider adding an explicit CAST)");
    }

    @Test
    public void numbersCoercion() {
        put(1);

        checkValue0("select coalesce(this, CAST(null as BIGINT)) from map", SqlColumnType.BIGINT, 1L);
    }

    @Test
    public void dateTimeValuesAndLiterals() {
        LocalDate localDate = LocalDate.of(2021, 1, 1);
        put(localDate);
        checkValue0("select coalesce(this, '2021-01-02') from map", SqlColumnType.DATE, localDate);

        LocalTime localTime = LocalTime.of(12, 0);
        put(localTime);
        checkValue0("select coalesce(this, '13:00') from map", SqlColumnType.TIME, localTime);

        LocalDateTime localDateTime = LocalDateTime.of(localDate, localTime);
        put(localDateTime);
        checkValue0("select coalesce(this, '2021-01-02T13:00') from map", SqlColumnType.TIMESTAMP, localDateTime);

        OffsetDateTime offsetDateTime = OffsetDateTime.of(localDateTime, ZoneOffset.ofHours(2));
        put(offsetDateTime);
        checkValue0("select coalesce(this, '2021-01-02T13:00+01:00') from map", SqlColumnType.TIMESTAMP_WITH_TIME_ZONE, offsetDateTime);
    }
}
