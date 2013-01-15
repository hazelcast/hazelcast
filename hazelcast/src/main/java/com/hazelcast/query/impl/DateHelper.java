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

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @mdogan 4/26/12
 */
final class DateHelper {

    static final String timestampFormat = "yyyy-MM-dd hh:mm:ss.SSS";
    static final String dateFormat = "EEE MMM dd HH:mm:ss zzz yyyy";
    static final String sqlDateFormat = "yyyy-mm-dd";

    static Date parseDate(final String value) {
        try {
            return getUtilDateFormat().parse(value);
        } catch (ParseException e) {
            return throwRuntimeParseException(value, e);
        }
    }

    static Timestamp parseTimeStamp(final String value) {
        try {
            return new Timestamp(getTimestampFormat().parse(value).getTime());
        } catch (ParseException e) {
            return throwRuntimeParseException(value, e);
        }
    }

    static java.sql.Date parseSqlDate(final String value) {
        try {
            return new java.sql.Date(getSqlDateFormat().parse(value).getTime());
        } catch (ParseException e) {
            return throwRuntimeParseException(value, e);
        }
    }

    static Date tryParse(final String value) {
        try {
            return getUtilDateFormat().parse(value);
        } catch (Exception ignored) {
        }

        try {
            return getTimestampFormat().parse(value);
        } catch (Exception ignored) {
        }

        try {
            return getSqlDateFormat().parse(value);
        } catch (Exception ignored) {
        }

        return throwRuntimeParseException(value, null);
    }

    private static <T> T throwRuntimeParseException(String value, Exception e) {
        throw new RuntimeException("Unable to parse date from value: '" + value
                + "' ! Valid formats are: '" + dateFormat + "', '" + timestampFormat
                + "' and '" + sqlDateFormat + "'.", e);
    }

    private static DateFormat getTimestampFormat() {
        return new SimpleDateFormat(timestampFormat);
    }

    private static DateFormat getSqlDateFormat() {
        return new SimpleDateFormat(sqlDateFormat);
    }

    private static DateFormat getUtilDateFormat() {
        return new SimpleDateFormat(dateFormat);
    }

    private DateHelper() {}
}
