/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

final class DateHelper {

    static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
    static final String DATE_FORMAT = "EEE MMM dd HH:mm:ss zzz yyyy";
    static final String SQL_DATE_FORMAT = "yyyy-MM-dd";
    static final String SQL_TIME_FORMAT = "HH:mm:ss";

    private DateHelper() {
    }

    static Date parseDate(final String value) {
        try {
            return getUtilDateFormat().parse(value);
        } catch (ParseException e) {
            return throwRuntimeParseException(value, e, DATE_FORMAT);
        }
    }

    static Timestamp parseTimeStamp(final String value) {
        try {
            return new Timestamp(getTimestampFormat().parse(value).getTime());
        } catch (ParseException e) {
            return throwRuntimeParseException(value, e, TIMESTAMP_FORMAT);
        }
    }

    static java.sql.Date parseSqlDate(final String value) {
        try {
            return new java.sql.Date(getSqlDateFormat().parse(value).getTime());
        } catch (ParseException e) {
            return throwRuntimeParseException(value, e, SQL_DATE_FORMAT);
        }
    }

    static java.sql.Time parseSqlTime(final String value) {
        try {
            return new Time(getSqlTimeFormat().parse(value).getTime());
        } catch (ParseException e) {
            return throwRuntimeParseException(value, e, SQL_TIME_FORMAT);
        }
    }

    private static <T> T throwRuntimeParseException(String value, Exception e, String... legalFormats) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < legalFormats.length; i++) {
            sb.append("'").append(legalFormats[i]).append("'");
            if (i < legalFormats.length - 2) {
                sb.append(", ");
            }
        }
        throw new RuntimeException("Unable to parse date from value: '" + value
                + "' ! Valid format are: " + sb.toString() + ".", e);
    }

    private static DateFormat getTimestampFormat() {
        return new SimpleDateFormat(TIMESTAMP_FORMAT, Locale.US);
    }

    private static DateFormat getSqlDateFormat() {
        return new SimpleDateFormat(SQL_DATE_FORMAT, Locale.US);
    }

    private static DateFormat getUtilDateFormat() {
        return new SimpleDateFormat(DATE_FORMAT, Locale.US);
    }

    private static DateFormat getSqlTimeFormat() {
        return new SimpleDateFormat(SQL_TIME_FORMAT, Locale.US);
    }

}
