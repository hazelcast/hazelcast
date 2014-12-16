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

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import static org.junit.Assert.assertEquals;

/**
 * @author mdogan 7/4/13
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class DateHelperTest {

    public static final String DATE_FORMAT = DateHelper.DATE_FORMAT;
    public static final String TIMESTAMP_FORMAT = DateHelper.TIMESTAMP_FORMAT;
    public static final String SQL_DATE_FORMAT = DateHelper.SQL_DATE_FORMAT;

    @Test
    public void testSqlDate() {
        final long now = System.currentTimeMillis();

        final java.sql.Date date1 = new java.sql.Date(now);
        final java.sql.Date date2 = DateHelper.parseSqlDate(date1.toString());

        Calendar cal1 = Calendar.getInstance(Locale.US);
        cal1.setTimeInMillis(date1.getTime());
        Calendar cal2 = Calendar.getInstance(Locale.US);
        cal2.setTimeInMillis(date2.getTime());

        assertEquals(cal1.get(Calendar.YEAR), cal2.get(Calendar.YEAR));
        assertEquals(cal1.get(Calendar.MONTH), cal2.get(Calendar.MONTH));
        assertEquals(cal1.get(Calendar.DAY_OF_MONTH), cal2.get(Calendar.DAY_OF_MONTH));
    }

    @Test
    public void testUtilDate() {
        final long now = System.currentTimeMillis();

        final Date date1 = new Date(now);
        final Date date2 = DateHelper.parseDate(new SimpleDateFormat(DateHelperTest.DATE_FORMAT, Locale.US).format(date1));

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
        final long now = System.currentTimeMillis();

        final Timestamp date1 = new Timestamp(now);
        final Timestamp date2 = DateHelper.parseTimeStamp(date1.toString());

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
    public void testTime() {
        final long now = System.currentTimeMillis();

        final Time time1 = new Time(now);
        final Time time2 = DateHelper.parseSqlTime(time1.toString());

        Calendar cal1 = Calendar.getInstance(Locale.US);
        cal1.setTimeInMillis(time1.getTime());
        Calendar cal2 = Calendar.getInstance(Locale.US);
        cal2.setTimeInMillis(time2.getTime());

        assertEquals(cal1.get(Calendar.HOUR_OF_DAY), cal2.get(Calendar.HOUR_OF_DAY));
        assertEquals(cal1.get(Calendar.MINUTE), cal2.get(Calendar.MINUTE));
        assertEquals(cal1.get(Calendar.SECOND), cal2.get(Calendar.SECOND));
    }
}

