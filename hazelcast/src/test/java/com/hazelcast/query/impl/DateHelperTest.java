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

package com.hazelcast.query.impl;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class DateHelperTest {

    public static final String DATE_FORMAT = DateHelper.DATE_FORMAT;
    public static final String TIMESTAMP_FORMAT = DateHelper.TIMESTAMP_FORMAT;
    public static final String SQL_DATE_FORMAT = DateHelper.SQL_DATE_FORMAT;
    public static final String SQL_TIME_FORMAT = DateHelper.SQL_TIME_FORMAT;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testSqlDate() {
        long now = System.currentTimeMillis();

        java.sql.Date date1 = new java.sql.Date(now);
        java.sql.Date date2 = DateHelper.parseSqlDate(date1.toString());

        assertSqlDatesEqual(date1, date2);
    }

    @Test
    public void testSqlDateWithLeadingZerosInMonthAndDay() throws Exception {
        // Given
        long expectedDateInMillis = new SimpleDateFormat(SQL_DATE_FORMAT)
                .parse("2003-01-04")
                .getTime();
        java.sql.Date expectedDate = new java.sql.Date(expectedDateInMillis);

        // When
        java.sql.Date actualDate = DateHelper.parseSqlDate(expectedDate.toString());

        // Then
        assertSqlDatesEqual(expectedDate, actualDate);
    }

    @Test
    public void testSqlDateWithTrailingZerosInMonthAndDay() throws Exception {
        // Given
        long expectedDateInMillis = new SimpleDateFormat(SQL_DATE_FORMAT)
                .parse("2000-10-20")
                .getTime();
        java.sql.Date expectedDate = new java.sql.Date(expectedDateInMillis);

        // When
        java.sql.Date actualDate = DateHelper.parseSqlDate(expectedDate.toString());

        // Then
        assertSqlDatesEqual(expectedDate, actualDate);
    }

    @Test
    public void testSqlDateFailsForInvalidDate() throws Exception {
        // Given
        String invalidDate = "Trust me, I am a date";

        // When
        thrown.expect(RuntimeException.class);
        thrown.expectCause(instanceOf(ParseException.class));
        DateHelper.parseSqlDate(invalidDate);

        // Then
        // No-op
    }

    private void assertSqlDatesEqual(java.sql.Date firstDate, java.sql.Date secondDate) {
        Calendar cal1 = Calendar.getInstance(Locale.US);
        cal1.setTimeInMillis(firstDate.getTime());
        Calendar cal2 = Calendar.getInstance(Locale.US);
        cal2.setTimeInMillis(secondDate.getTime());

        assertEquals(cal1.get(Calendar.YEAR), cal2.get(Calendar.YEAR));
        assertEquals(cal1.get(Calendar.MONTH), cal2.get(Calendar.MONTH));
        assertEquals(cal1.get(Calendar.DAY_OF_MONTH), cal2.get(Calendar.DAY_OF_MONTH));
    }

    @Test
    public void testUtilDate() {
        long now = System.currentTimeMillis();

        Date date1 = new Date(now);
        Date date2 = DateHelper.parseDate(new SimpleDateFormat(DateHelperTest.DATE_FORMAT, Locale.US).format(date1));

        Calendar cal1 = Calendar.getInstance(Locale.US);
        cal1.setTimeInMillis(date1.getTime());
        Calendar cal2 = Calendar.getInstance(Locale.US);
        cal2.setTimeInMillis(date2.getTime());

        assertEquals(cal1.get(Calendar.YEAR), cal2.get(Calendar.YEAR));
        assertEquals(cal1.get(Calendar.MONTH), cal2.get(Calendar.MONTH));
        assertEquals(cal1.get(Calendar.DAY_OF_MONTH), cal2.get(Calendar.DAY_OF_MONTH));
        assertEquals(cal1.get(Calendar.HOUR_OF_DAY), cal2.get(Calendar.HOUR_OF_DAY));
        assertEquals(cal1.get(Calendar.MINUTE), cal2.get(Calendar.MINUTE));
        assertEquals(cal1.get(Calendar.SECOND), cal2.get(Calendar.SECOND));
    }

    @Test
    public void testTimestamp() {
        long now = System.currentTimeMillis();

        Timestamp date1 = new Timestamp(now);
        Timestamp date2 = DateHelper.parseTimeStamp(date1.toString());

        assertTimestampsEqual(date1, date2);
    }

    @Test
    public void testTimestampWithLeadingZeros() throws Exception {
        // Given
        Timestamp expectedTimestamp = new Timestamp(new SimpleDateFormat(TIMESTAMP_FORMAT)
                .parse("2000-01-02 03:04:05.006")
                .getTime());

        // When
        Timestamp actualTimestamp = DateHelper.parseTimeStamp(expectedTimestamp.toString());

        // Then
        assertTimestampsEqual(expectedTimestamp, actualTimestamp);
    }

    @Test
    public void testTimestampWithTrailingZeros() throws Exception {
        // Given
        Timestamp expectedTimestamp = new Timestamp(new SimpleDateFormat(TIMESTAMP_FORMAT)
                .parse("2010-10-20 10:20:30.040")
                .getTime());

        // When
        Timestamp actualTimestamp = DateHelper.parseTimeStamp(expectedTimestamp.toString());

        // Then
        assertTimestampsEqual(expectedTimestamp, actualTimestamp);
    }

    @Test
    public void testTimestampFailsForInvalidValue() throws Exception {
        // Given
        String invalidTimestamp = "Quid temporem est";

        // When
        thrown.expectCause(instanceOf(ParseException.class));
        DateHelper.parseTimeStamp(invalidTimestamp);

        // Then
        // No-op
    }

    private Matcher<Throwable> instanceOf(Class<?> exceptionClass) {
        return Matchers.instanceOf(exceptionClass);
    }

    private void assertTimestampsEqual(Timestamp firstTimestamp, Timestamp secondTimestamp) {
        Calendar cal1 = Calendar.getInstance(Locale.US);
        cal1.setTimeInMillis(firstTimestamp.getTime());
        Calendar cal2 = Calendar.getInstance(Locale.US);
        cal2.setTimeInMillis(secondTimestamp.getTime());

        assertEquals(cal1.get(Calendar.YEAR), cal2.get(Calendar.YEAR));
        assertEquals(cal1.get(Calendar.MONTH), cal2.get(Calendar.MONTH));
        assertEquals(cal1.get(Calendar.DAY_OF_MONTH), cal2.get(Calendar.DAY_OF_MONTH));
        assertEquals(cal1.get(Calendar.HOUR_OF_DAY), cal2.get(Calendar.HOUR_OF_DAY));
        assertEquals(cal1.get(Calendar.MINUTE), cal2.get(Calendar.MINUTE));
        assertEquals(cal1.get(Calendar.SECOND), cal2.get(Calendar.SECOND));
    }

    @Test
    public void testTime() {
        long now = System.currentTimeMillis();

        Time time1 = new Time(now);
        Time time2 = DateHelper.parseSqlTime(time1.toString());

        assertSqlTimesEqual(time1, time2);
    }

    @Test
    public void testTimeWithLeadingZeros() throws Exception {
        // Given
        Time expectedTime = new Time(
                new SimpleDateFormat(SQL_TIME_FORMAT)
                        .parse("01:02:03")
                        .getTime()
        );

        // When
        Time actualTime = DateHelper.parseSqlTime(expectedTime.toString());

        // Then
        assertSqlTimesEqual(expectedTime, actualTime);
    }

    @Test
    public void testTimeWithTrailingZeros() throws Exception {
        // Given
        Time expectedTime = new Time(
                new SimpleDateFormat(SQL_TIME_FORMAT)
                        .parse("10:20:30")
                        .getTime()
        );

        // When
        Time actualTime = DateHelper.parseSqlTime(expectedTime.toString());

        // Then
        assertSqlTimesEqual(expectedTime, actualTime);
    }

    @Test
    public void testTimeFailsForInvalidValue() throws Exception {
        // Given
        String invalidTime = "Time is now";

        // When
        thrown.expectCause(instanceOf(ParseException.class));
        DateHelper.parseSqlTime(invalidTime);

        // Then
        // No-op
    }

    private void assertSqlTimesEqual(Time firstTime, Time secondTime) {
        Calendar cal1 = Calendar.getInstance(Locale.US);
        cal1.setTimeInMillis(firstTime.getTime());
        Calendar cal2 = Calendar.getInstance(Locale.US);
        cal2.setTimeInMillis(secondTime.getTime());

        assertEquals(cal1.get(Calendar.HOUR_OF_DAY), cal2.get(Calendar.HOUR_OF_DAY));
        assertEquals(cal1.get(Calendar.MINUTE), cal2.get(Calendar.MINUTE));
        assertEquals(cal1.get(Calendar.SECOND), cal2.get(Calendar.SECOND));
    }
}
