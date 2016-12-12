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

package com.hazelcast.internal.diagnostics;

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

    protected final StringBuilder sb = new StringBuilder();
    protected int sectionLevel = -1;
    // lots of stuff to write a date without generating litter
    private final Calendar calendar = new GregorianCalendar(TimeZone.getDefault());
    private final Date date = new Date();


    public abstract void startSection(String name);

    public abstract void endSection();

    public abstract void writeEntry(String s);

    public abstract void writeKeyValueEntry(String key, String value);

    public abstract void writeKeyValueEntry(String key, double value);

    public abstract void writeKeyValueEntry(String key, long value);

    public abstract void writeKeyValueEntry(String key, boolean value);

    protected void clean() {
        sb.setLength(0);
    }

    public int length() {
        return sb.length();
    }

    public void copyInto(char[] target) {
        sb.getChars(0, sb.length(), target, 0);
    }

    // we can't rely on DateFormat since it generates a ton of garbage
    protected void appendDateTime() {
        date.setTime(System.currentTimeMillis());
        calendar.setTime(date);
        appendDate();
        sb.append(' ');
        appendTime();
    }

    private void appendDate() {
        sb.append(calendar.get(DAY_OF_MONTH));
        sb.append('-');
        sb.append(calendar.get(MONTH) + 1);
        sb.append('-');
        sb.append(calendar.get(YEAR));
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private void appendTime() {
        int hour = calendar.get(HOUR_OF_DAY);
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
}
