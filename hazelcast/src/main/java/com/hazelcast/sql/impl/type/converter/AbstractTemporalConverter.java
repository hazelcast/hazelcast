/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.type.converter;

import com.hazelcast.sql.impl.type.QueryDataTypeFamily;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * Base converter for date/time types.
 */
public abstract class AbstractTemporalConverter extends Converter {

    public static final ZoneId DEFAULT_ZONE = ZoneId.systemDefault();

    protected AbstractTemporalConverter(int id, QueryDataTypeFamily typeFamily) {
        super(id, typeFamily);
    }

    /**
     * Convert DATE to TIMESTAMP as per ANSI standard.
     *
     * @param date Date.
     * @return Timestamp with the same date at midnight.
     */
    protected static LocalDateTime dateToTimestamp(LocalDate date) {
        return date.atStartOfDay();
    }

    /**
     * Convert TIME to TIMESTAMP as per ANSI standard.
     *
     * @param time Time.
     * @return Timestamp with the same time and current date.
     */
    protected static LocalDateTime timeToTimestamp(LocalTime time) {
        return LocalDateTime.of(LocalDate.now(), time);
    }

    /**
     * Convert TIMESTAMP to TIMESTAMP WITH TIMEZONE as per ANSI standard.
     *
     * @param timestamp Original timestamp.
     * @return Timestamp with timezone.
     */
    protected static OffsetDateTime timestampToTimestampWithTimezone(LocalDateTime timestamp) {
        return ZonedDateTime.of(timestamp, DEFAULT_ZONE).toOffsetDateTime();
    }

    /**
     * Convert TIMESTAMP WITH TIMEZONE to TIMESTAMP as per ANSI standard.
     *
     * @param timestampWithTimezone Original timestamp with timezone.
     * @return Timestamp.
     */
    protected static LocalDateTime timestampWithTimezoneToTimestamp(OffsetDateTime timestampWithTimezone) {
        return timestampWithTimezone.toLocalDateTime();
    }
}
