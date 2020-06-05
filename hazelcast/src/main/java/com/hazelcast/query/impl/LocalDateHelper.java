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
package com.hazelcast.query.impl;

import static java.time.format.DateTimeFormatter.BASIC_ISO_DATE;
import static java.time.format.DateTimeFormatter.ISO_DATE;
import static java.time.format.DateTimeFormatter.ISO_DATE_TIME;
import static java.time.format.DateTimeFormatter.ISO_INSTANT;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;
import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE;
import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static java.time.format.DateTimeFormatter.ISO_ZONED_DATE_TIME;
import static java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME;
import static java.time.format.DateTimeFormatter.ofLocalizedDate;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.FormatStyle;
import java.time.temporal.TemporalAccessor;

public final class LocalDateHelper {

    private static final FormatStyle[] FORMAT_STYLES = FormatStyle.values();

    private static final DateTimeFormatter[] DATETIME_FORMATTERS = { ISO_LOCAL_DATE, ISO_OFFSET_DATE, ISO_DATE,
            ISO_LOCAL_DATE_TIME, ISO_OFFSET_DATE_TIME, ISO_DATE_TIME, ISO_ZONED_DATE_TIME, ISO_INSTANT, BASIC_ISO_DATE,
            RFC_1123_DATE_TIME };

    private static final String[] DATE_FORMATS = new String[] { "yyyy-MM-dd", "yyyy/MM/dd", "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd HH:mm", "dd MMM yyyy", "dd MMM yyyy HH:mm", "dd MMM yyyy HH:mm", "dd MMM yyyy HH:mm:ss",
            "dd MMM yyyy HH:mm:ss", "MM/yy", "dd MMM yyyy, HH:mm:ss", "E MMM dd HH:mm:ss Z yyyy",
            "dd mmm yyyy, HH:mm a", "yyyy-MM-dd'T'hh:mm:ss" };

    private LocalDateHelper() {
    }

    public static LocalDate toLocalDate(String dateAsString) {
        return LocalDate.from(getFormatter(dateAsString));
    }

    public static LocalDateTime toLocalDateTime(String dateAsString) {
        return LocalDateTime.from(getFormatter(dateAsString));
    }

    private static TemporalAccessor getFormatter(String dateAsString) {
        for (DateTimeFormatter formatter : DATETIME_FORMATTERS) {
            try {
                return formatter.parse(dateAsString);
            } catch (DateTimeParseException ignore) {
            }
        }
        for (FormatStyle formatStyle : FORMAT_STYLES) {
            DateTimeFormatter formatter = ofLocalizedDate(formatStyle);
            try {
                return formatter.parse(dateAsString);
            } catch (DateTimeParseException ignore) {
            }
        }
        for (String format : DATE_FORMATS) {
            try {
                return DateTimeFormatter.ofPattern(format).parse(dateAsString);
            } catch (DateTimeParseException ignore) {
            }
        }
        throw new RuntimeException("Unable to parse date from value: '" + dateAsString + "' ! ");
    }

}
