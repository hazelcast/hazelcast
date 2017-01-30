/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import static java.util.Calendar.DAY_OF_MONTH;
import static java.util.Calendar.HOUR_OF_DAY;
import static java.util.Calendar.MINUTE;
import static java.util.Calendar.MONTH;
import static java.util.Calendar.SECOND;
import static java.util.Calendar.YEAR;

/**
 * A writer like structure dedicated for the {@link DiagnosticsPlugin} rendering.
 */
public abstract class DiagnosticsLogWriter {

    //32 chars should be more than enough to encode primitives
    private static final int CHARS_LENGTH = 32;

    protected int sectionLevel = -1;
    private PrintWriter printWriter;

    // lots of stuff to write a date without generating litter
    private final Calendar calendar = new GregorianCalendar(TimeZone.getDefault());
    private final Date date = new Date();

    // used for encoding primitives.
    private char[] chars = new char[CHARS_LENGTH];

    // used to write primitives without causing litter.
    private StringBuilder stringBuilder = new StringBuilder();

    public abstract void startSection(String name);

    public abstract void endSection();

    public abstract void writeEntry(String s);

    public abstract void writeKeyValueEntry(String key, String value);

    public abstract void writeKeyValueEntry(String key, double value);

    public abstract void writeKeyValueEntry(String key, long value);

    public abstract void writeKeyValueEntry(String key, boolean value);

    protected void init(PrintWriter printWriter) {
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
    protected void appendDateTime() {
        date.setTime(System.currentTimeMillis());
        calendar.setTime(date);
        appendDate();
        write(' ');
        appendTime();
    }

    private void appendDate() {
        write(calendar.get(DAY_OF_MONTH));
        write('-');
        write(calendar.get(MONTH) + 1);
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
