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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.Date;

public final class LocalDateHelper {

    private static final String[] DATE_FORMATS = new String[] { "yyyy-MM-dd", "yyyy/MM/dd", "yyyy-MM-dd hh:mm:ss",
            "yyyy-MM-dd hh:mm", "dd MMM yyyy", "dd MMM yyyy hh:mm", "dd MMM yyyy HH:mm", "dd MMM yyyy hh:mm:ss",
            "dd MMM yyyy HH:mm:ss", "MM/yy", "dd MMM yyyy, hh:mm:ss", "E MMM dd HH:mm:ss Z yyyy", "dd mmm yyyy, hh:mm a",
            "yyyy-MM-dd'T'hh:mm:ss" };

    private LocalDateHelper() {
    }

    public static Date toDate(String dateAsString) {
        for (String format : DATE_FORMATS) {
            try {
                return new SimpleDateFormat(format).parse(dateAsString);
            } catch (ParseException ignore) {
                // DO NOTHING
            }
        }
        throw new RuntimeException("Unable to parse date from value: '" + dateAsString + "' ! ");
    }

    public static LocalDate toLocalDate(String dateAsString) {
        Date d = toDate(dateAsString);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(d);
        return LocalDate.of(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DATE));
    }

    public static LocalDateTime toLocalDateTime(String dateAsString) {
        Date d = toDate(dateAsString);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(d);
        return LocalDateTime.of(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DATE),
                calendar.get(Calendar.HOUR), calendar.get(Calendar.MINUTE), calendar.get(Calendar.SECOND));
    }

}
