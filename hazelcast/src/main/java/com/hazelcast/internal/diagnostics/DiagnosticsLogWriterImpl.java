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

package com.hazelcast.internal.diagnostics;

import java.io.PrintWriter;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import static com.hazelcast.internal.util.StringUtil.LINE_SEPARATOR;
import static com.hazelcast.internal.util.StringUtil.LOCALE_INTERNAL;
import static java.util.Calendar.DAY_OF_MONTH;
import static java.util.Calendar.HOUR_OF_DAY;
import static java.util.Calendar.MINUTE;
import static java.util.Calendar.MONTH;
import static java.util.Calendar.SECOND;
import static java.util.Calendar.YEAR;

/**
 * A writer like structure dedicated for the {@link DiagnosticsPlugin}
 * rendering.
 */
public class DiagnosticsLogWriterImpl implements DiagnosticsLogWriter {

    private static final String STR_LONG_MIN_VALUE = String.format(LOCALE_INTERNAL, "%,d", Long.MIN_VALUE);

    private static final char[] DIGITS = new char[]{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};

    private static final String[] INDENTS = new String[]{
            LINE_SEPARATOR + "                          ",
            LINE_SEPARATOR + "                                  ",
            LINE_SEPARATOR + "                                          ",
            LINE_SEPARATOR + "                                                  ",
            LINE_SEPARATOR + "                                                          ",
    };

    // 32 chars should be more than enough to encode primitives
    private static final int CHARS_LENGTH = 32;

    private final ILogger logger;

    private final StringBuilder tmpSb = new StringBuilder();
    private final boolean includeEpochTime;

    private int sectionLevel = -1;
    private PrintWriter printWriter;

    // lots of stuff to write a date without generating litter
    private final Calendar calendar = new GregorianCalendar(TimeZone.getDefault());
    private final Date date = new Date();

    // used for encoding primitives
    private char[] chars = new char[CHARS_LENGTH];

    // used to write primitives without causing litter
    private StringBuilder stringBuilder = new StringBuilder();

    public DiagnosticsLogWriterImpl() {
        this(false, null);
    }

    public DiagnosticsLogWriterImpl(boolean includeEpochTime, ILogger logger) {
        this.includeEpochTime = includeEpochTime;
        this.logger = logger != null ? logger : Logger.getLogger(getClass());
    }

    @Override
    public void writeSectionKeyValue(String sectionName, long timeMillis, String key, long value) {
        startSection(sectionName, timeMillis);
        write(key);
        write('=');
        write(value);
        endSection();
    }

    @Override
    public void writeSectionKeyValue(String sectionName, long timeMillis, String key, double value) {
        startSection(sectionName, timeMillis);
        write(key);
        write('=');
        write(value);
        endSection();
    }

    @Override
    public void writeSectionKeyValue(String sectionName, long timeMillis, String key, String value) {
        startSection(sectionName, timeMillis);
        write(key);
        write('=');
        write(value);
        endSection();
    }

    @Override
    public void startSection(String sectionName) {
        startSection(sectionName, System.currentTimeMillis());
    }

    public void startSection(String name, long timeMillis) {
        if (sectionLevel == -1) {
            appendDateTime(timeMillis);
            write(' ');

            if (includeEpochTime) {
                write(timeMillis);
                write(' ');
            }
        }

        if (sectionLevel >= 0) {
            write(INDENTS[sectionLevel]);
        }

        write(name);
        write('[');
        if (sectionLevel < INDENTS.length - 1) {
            sectionLevel++;
        } else {
            logger.warning("Diagnostics writer SectionLevel has overflown.", new Exception("Dumping stack trace"));
            sectionLevel = INDENTS.length - 1;
        }
    }

    @Override
    public void endSection() {
        write(']');
        if (sectionLevel > -1) {
            sectionLevel--;
        } else {
            logger.warning("Diagnostics writer SectionLevel has underflown.", new Exception("Dumping stack trace"));
            sectionLevel = -1;
        }
        if (sectionLevel == -1) {
            write(LINE_SEPARATOR);
        }
    }

    @Override
    public void writeEntry(String s) {
        if (sectionLevel >= 0) {
            write(INDENTS[sectionLevel]);
        }
        write(s);
    }

    @Override
    public void writeKeyValueEntry(String key, String value) {
        writeKeyValueHead(key);
        write(value);
    }

    // we can't rely on NumberFormat, since it generates a ton of garbage
    @SuppressWarnings("checkstyle:magicnumber")
    void writeLong(long value) {
        if (value == Long.MIN_VALUE) {
            write(STR_LONG_MIN_VALUE);
            return;
        }

        if (value < 0) {
            write('-');
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
            write(c);
        }
    }

    @Override
    public void writeKeyValueEntry(String key, double value) {
        writeKeyValueHead(key);
        write(value);
    }

    @Override
    public void writeKeyValueEntry(String key, long value) {
        writeKeyValueHead(key);
        writeLong(value);
    }

    @Override
    public void writeKeyValueEntry(String key, boolean value) {
        writeKeyValueHead(key);
        write(value);
    }

    @Override
    public void writeKeyValueEntryAsDateTime(String key, long epochMillis) {
        writeKeyValueHead(key);
        appendDateTime(epochMillis);
    }

    private void writeKeyValueHead(String key) {
        if (sectionLevel >= 0) {
            write(INDENTS[sectionLevel]);
        }
        write(key);
        write('=');
    }

    /**
     * Reset the sectionLevel. A proper rendering of a plugin should always
     * return this value to -1; but in case of an exception while rendering,
     * the section level isn't reset and subsequent renderings of plugins
     * will run into an IndexOutOfBoundsException.
     * https://github.com/hazelcast/hazelcast/issues/14973
     */
    public void resetSectionLevel() {
        sectionLevel = -1;
    }

    public void init(PrintWriter printWriter) {
        sectionLevel = -1;
        this.printWriter = printWriter;
    }

    protected DiagnosticsLogWriter write(char c) {
        printWriter.write(c);
        return this;
    }

    protected DiagnosticsLogWriter write(int i) {
        stringBuilder.append(i);
        flushSb();
        return this;
    }

    protected DiagnosticsLogWriter write(double i) {
        stringBuilder.append(i);
        flushSb();
        return this;
    }

    protected DiagnosticsLogWriter write(long i) {
        stringBuilder.append(i);
        flushSb();
        return this;
    }

    private void flushSb() {
        int length = stringBuilder.length();
        stringBuilder.getChars(0, length, chars, 0);
        printWriter.write(chars, 0, length);
        stringBuilder.setLength(0);
    }

    protected DiagnosticsLogWriter write(boolean b) {
        write(b ? "true" : "false");
        return this;
    }

    protected DiagnosticsLogWriter write(String s) {
        printWriter.write(s == null ? "null" : s);
        return this;
    }

    // we can't rely on DateFormat since it generates a ton of garbage
    private void appendDateTime(long epochMillis) {
        date.setTime(epochMillis);
        calendar.setTime(date);
        appendDate();
        write(' ');
        appendTime();
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private void appendDate() {
        int dayOfMonth = calendar.get(DAY_OF_MONTH);
        if (dayOfMonth < 10) {
            write('0');
        }
        write(dayOfMonth);
        write('-');
        int month = calendar.get(MONTH) + 1;
        if (month < 10) {
            write('0');
        }
        write(month);
        write('-');
        write(calendar.get(YEAR));
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private void appendTime() {
        int hour = calendar.get(HOUR_OF_DAY);
        if (hour < 10) {
            write('0');
        }
        write(hour);
        write(':');
        int minute = calendar.get(MINUTE);
        if (minute < 10) {
            write('0');
        }
        write(minute);
        write(':');
        int second = calendar.get(SECOND);
        if (second < 10) {
            write('0');
        }
        write(second);
    }
}
