/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.monitors;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import static com.hazelcast.util.StringUtil.LINE_SEPARATOR;
import static java.util.Calendar.DAY_OF_MONTH;
import static java.util.Calendar.HOUR;
import static java.util.Calendar.MINUTE;
import static java.util.Calendar.MONTH;
import static java.util.Calendar.SECOND;
import static java.util.Calendar.YEAR;

/**
 * A {@link PerformanceLogWriter} that writes over multiple lines. Useful for human reading.
 */
class MultiLinePerformanceLogWriter extends PerformanceLogWriter {

    private static final String STR_LONG_MIN_VALUE = String.format("%,d", Long.MIN_VALUE);

    private static final char[] DIGITS = new char[]{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};

    private static final String[] INDENTS = new String[]{
            LINE_SEPARATOR + "                          ",
            LINE_SEPARATOR + "                                  ",
            LINE_SEPARATOR + "                                          ",
            LINE_SEPARATOR + "                                                  ", };

    private int indentLevel = -1;
    private final StringBuffer tmpSb = new StringBuffer();
    // lots of stuff to render a date without generating litter
    private final Calendar calendar = new GregorianCalendar(TimeZone.getDefault());
    private final Date date = new Date();

    @Override
    public void startSection(String name) {
        if (indentLevel >= 0) {
            sb.append(INDENTS[indentLevel]);
        }

        sb.append(name);
        sb.append('[');
        indentLevel++;
    }

    @Override
    public void endSection() {
        sb.append(']');
        indentLevel--;
    }

    @Override
    public void writeEntry(String s) {
        sb.append(INDENTS[indentLevel]);
        sb.append(s);
    }

    @Override
    public void writeKeyValueEntry(String key, String value) {
        writeKeyValueHead(key);
        sb.append(value);
    }

    // we can't rely on NumberFormat since it generates a ton of garbage
    @SuppressWarnings("checkstyle:magicnumber")
    void writeLong(long value) {
        if (value == Long.MIN_VALUE) {
            sb.append(STR_LONG_MIN_VALUE);
            return;
        }

        if (value < 0) {
            sb.append('-');
            value = -value;
        }

        int digitsWithoutComma = 0;
        tmpSb.setLength(0);
        do {
            digitsWithoutComma++;
            if (digitsWithoutComma == 4) {
                tmpSb.append(',');
                digitsWithoutComma = 1;
            }

            int mod = (int) (value % 10);
            tmpSb.append(DIGITS[mod]);
            value = value / 10;
        } while (value > 0);

        for (int k = tmpSb.length() - 1; k >= 0; k--) {
            char c = tmpSb.charAt(k);
            sb.append(c);
        }
    }

    @Override
    public void writeKeyValueEntry(String key, double value) {
        writeKeyValueHead(key);
        sb.append(value);
    }

    @Override
    public void writeKeyValueEntry(String key, long value) {
        writeKeyValueHead(key);
        writeLong(value);
    }

    @Override
    public void writeKeyValueEntry(String key, boolean value) {
        writeKeyValueHead(key);
        sb.append(value);
    }

    private void writeKeyValueHead(String key) {
        sb.append(INDENTS[indentLevel]);
        sb.append(key);
        sb.append('=');
    }

    @Override
    void write(PerformanceMonitorPlugin plugin) {
        clean();

        appendDateTime();
        sb.append(' ');
        plugin.run(this);
        sb.append(LINE_SEPARATOR);
    }

    // we can't rely on DateFormat since it generates a ton of garbage
    private void appendDateTime() {
        date.setTime(System.currentTimeMillis());
        calendar.setTime(date);
        appendDate();
        sb.append(' ');
        appendTime();
    }

    private void appendDate() {
        sb.append(calendar.get(DAY_OF_MONTH));
        sb.append('-');
        sb.append(calendar.get(MONTH));
        sb.append('-');
        sb.append(calendar.get(YEAR));
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private void appendTime() {
        int hour = calendar.get(HOUR);
        if (hour < 10) {
            sb.append('0');
        }
        sb.append(hour);
        sb.append(':');
        int minute = calendar.get(MINUTE);
        if (minute < 10) {
            sb.append('0');
        }
        sb.append(minute);
        sb.append(':');
        int second = calendar.get(SECOND);
        if (second < 10) {
            sb.append('0');
        }
        sb.append(second);
    }

    @Override
    protected void clean() {
        super.clean();
        indentLevel = -1;
    }
}
