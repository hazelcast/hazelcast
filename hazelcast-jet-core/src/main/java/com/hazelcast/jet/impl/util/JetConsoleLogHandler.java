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

package com.hazelcast.jet.impl.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.StreamHandler;

public class JetConsoleLogHandler extends StreamHandler {

    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_BLACK = "\u001B[30m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_PURPLE = "\u001B[35m";
    public static final String ANSI_CYAN = "\u001B[36m";
    public static final String ANSI_WHITE = "\u001B[37m";

    public JetConsoleLogHandler() {
        super(System.out, new JetLogFormatter());
    }

    @Override
    public void publish(LogRecord record) {
        // synchronized method call!
        super.publish(record);
        flush();
    }

    /**
     * Since this handler uses {@code System.out}, just flush it without
     * closing.
     */
    @Override
    public void close() {
        flush();
    }

    private static class JetLogFormatter extends Formatter {

        private static final int ERROR_LEVEL = 1000;
        private static final int WARNING_LEVEL = 900;
        private static final int INFO_LEVEL = 800;
        private static final int DEBUG_LEVEL = 500;
        private static final int TRACE_LEVEL = 400;

        @Override
        public String format(LogRecord record) {
            String loggerName = abbreviateLoggerName(record.getLoggerName());
            String message = record.getMessage();
            return String.format("%s [%s%5s%s] [%s%s%s] %s%s",
                    Util.toLocalTime(record.getMillis()),
                    getLevelColor(record.getLevel()),
                    getLevel(record.getLevel()),
                    ANSI_RESET,
                    ANSI_BLUE,
                    loggerName,
                    ANSI_RESET,
                    message,
                    record.getThrown() == null ? System.lineSeparator() : getExceptionString(record));
        }

        private static String abbreviateLoggerName(String name) {
            return name.replaceAll("\\B\\w+\\.", ".");
        }

        private static String getExceptionString(LogRecord record) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            pw.write(System.lineSeparator());
            pw.write(ANSI_RED);
            record.getThrown().printStackTrace(new PrintWriter(sw));
            pw.write(ANSI_RESET);
            return sw.toString();
        }

        private static String getLevelColor(Level level) {
            switch (level.intValue()) {
                case ERROR_LEVEL:
                    return ANSI_RED;
                case WARNING_LEVEL:
                    return ANSI_YELLOW;
                case INFO_LEVEL:
                    return ANSI_GREEN;
                default:
                    return ANSI_RESET;
            }
        }

        private static String getLevel(Level level) {
            switch (level.intValue()) {
                case ERROR_LEVEL:
                    return  "ERROR";
                case WARNING_LEVEL:
                    return  "WARN";
                case INFO_LEVEL:
                    return  "INFO";
                case DEBUG_LEVEL:
                    return  "DEBUG";
                case TRACE_LEVEL:
                    return  "TRACE";
                default:
                    return level.toString();
            }
        }
    }
}
