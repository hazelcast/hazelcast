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

package com.hazelcast.logging;

import com.hazelcast.logging.impl.InternalLogger;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;

import java.util.logging.Level;
import java.util.logging.LogRecord;

public class Log4jFactory extends LoggerFactorySupport implements LoggerFactory {

    @Override
    protected ILogger createLogger(String name) {
        final Logger l = Logger.getLogger(name);
        return new Log4jLogger(l);
    }

    static class Log4jLogger extends AbstractLogger implements InternalLogger {

        private final Logger logger;
        private final Level level;

        Log4jLogger(Logger logger) {
            this.logger = logger;
            org.apache.log4j.Level log4jLevel = logger.getLevel();
            this.level = toStandardLevel(log4jLevel);
        }

        @Override
        public void setLevel(Level level) {
            logger.setLevel(toLog4jLevel(level));
        }

        @Override
        public void log(Level level, String message) {
            logger.log(toLog4jLevel(level), message);
        }

        @Override
        public void log(Level level, String message, Throwable thrown) {
            logger.log(toLog4jLevel(level), message, thrown);
        }

        @Override
        public Level getLevel() {
            return level;
        }

        @Override
        public boolean isLoggable(Level level) {
            return level != Level.OFF && logger.isEnabledFor(toLog4jLevel(level));
        }

        @Override
        public void log(LogEvent logEvent) {
            LogRecord logRecord = logEvent.getLogRecord();
            Level eventLevel = logRecord.getLevel();
            if (eventLevel == Level.OFF) {
                return;
            }
            String name = logEvent.getLogRecord().getLoggerName();
            org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(name);
            org.apache.log4j.Level level = toLog4jLevel(eventLevel);
            String message = logRecord.getMessage();
            Throwable throwable = logRecord.getThrown();
            logger.callAppenders(new LoggingEvent(name, logger, level, message, throwable));
        }

        private static org.apache.log4j.Level toLog4jLevel(Level level) {
            return level == Level.FINEST  ? org.apache.log4j.Level.TRACE
                 : level == Level.FINE    ? org.apache.log4j.Level.DEBUG
                 : level == Level.INFO    ? org.apache.log4j.Level.INFO
                 : level == Level.WARNING ? org.apache.log4j.Level.WARN
                 : level == Level.SEVERE  ? org.apache.log4j.Level.ERROR
                 : level == Level.CONFIG  ? org.apache.log4j.Level.INFO
                 : level == Level.FINER   ? org.apache.log4j.Level.DEBUG
                 : level == Level.OFF     ? org.apache.log4j.Level.OFF
                 : org.apache.log4j.Level.INFO;
        }

        private static Level toStandardLevel(org.apache.log4j.Level log4jLevel) {
            return log4jLevel == org.apache.log4j.Level.TRACE ? Level.FINEST
                 : log4jLevel == org.apache.log4j.Level.DEBUG ? Level.FINE
                 : log4jLevel == org.apache.log4j.Level.INFO  ? Level.INFO
                 : log4jLevel == org.apache.log4j.Level.WARN  ? Level.WARNING
                 : log4jLevel == org.apache.log4j.Level.ERROR ? Level.SEVERE
                 : log4jLevel == org.apache.log4j.Level.FATAL ? Level.SEVERE
                 : log4jLevel == org.apache.log4j.Level.OFF   ? Level.OFF
                 : Level.INFO;
        }
    }
}
