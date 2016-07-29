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

import static com.hazelcast.util.StringUtil.LINE_SEPARATOR;
import static com.hazelcast.util.StringUtil.LOCALE_INTERNAL;

/**
 * A {@link DiagnosticsLogWriter} that writes over multiple lines. Useful for human reading.
 */
class MultiLineDiagnosticsLogWriter extends DiagnosticsLogWriter {

    private static final String STR_LONG_MIN_VALUE = String.format(LOCALE_INTERNAL, "%,d", Long.MIN_VALUE);

    private static final char[] DIGITS = new char[]{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};

    private static final String[] INDENTS = new String[]{
            LINE_SEPARATOR + "                          ",
            LINE_SEPARATOR + "                                  ",
            LINE_SEPARATOR + "                                          ",
            LINE_SEPARATOR + "                                                  ", };

    private final StringBuffer tmpSb = new StringBuffer();

    @Override
    public void startSection(String name) {
        if (sectionLevel == -1) {
            appendDateTime();
            sb.append(' ');
        }

        if (sectionLevel >= 0) {
            sb.append(INDENTS[sectionLevel]);
        }

        sb.append(name);
        sb.append('[');
        sectionLevel++;
    }

    @Override
    public void endSection() {
        sb.append(']');
        sectionLevel--;

        if (sectionLevel == -1) {
            sb.append(LINE_SEPARATOR);
        }
    }

    @Override
    public void writeEntry(String s) {
        sb.append(INDENTS[sectionLevel]);
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
        sb.append(INDENTS[sectionLevel]);
        sb.append(key);
        sb.append('=');
    }
}
