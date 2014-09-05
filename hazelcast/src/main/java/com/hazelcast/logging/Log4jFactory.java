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

package com.hazelcast.logging;

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

    static class Log4jLogger extends AbstractLogger {
        private final Logger logger;
        private final Level level;

        public Log4jLogger(Logger logger) {
            this.logger = logger;
            if (logger.getLevel() == org.apache.log4j.Level.DEBUG) {
                level = Level.FINEST;
            } else if (logger.getLevel() == org.apache.log4j.Level.INFO) {
                level = Level.INFO;
            } else if (logger.getLevel() == org.apache.log4j.Level.WARN) {
                level = Level.WARNING;
            } else if (logger.getLevel() == org.apache.log4j.Level.FATAL) {
                level = Level.SEVERE;
            } else {
                level = Level.INFO;
            }
        }

        @Override
        public void log(Level level, String message) {
            if (Level.FINEST == level) {
                logger.debug(message);
            } else if (Level.SEVERE == level) {
                logger.fatal(message);
            } else if (Level.WARNING == level) {
                logger.warn(message);
            } else {
                logger.info(message);
            }
        }

        @Override
        public Level getLevel() {
            return level;
        }

        @Override
        public boolean isLoggable(Level level) {
            if (Level.OFF == level) {
                return false;
            } else if (Level.FINEST == level) {
                return logger.isDebugEnabled();
            } else if (Level.WARNING == level) {
                return logger.isEnabledFor(org.apache.log4j.Level.WARN);
            } else if (Level.SEVERE == level) {
                return logger.isEnabledFor(org.apache.log4j.Level.FATAL);
            } else {
                return logger.isEnabledFor(org.apache.log4j.Level.INFO);
            }
        }

        @Override
        public void log(Level level, String message, Throwable thrown) {
            if (Level.FINEST == level) {
                logger.debug(message, thrown);
            } else if (Level.WARNING == level) {
                logger.warn(message, thrown);
            } else if (Level.SEVERE == level) {
                logger.fatal(message, thrown);
            } else {
                logger.info(message, thrown);
            }
        }

        @Override
        public void log(LogEvent logEvent) {
            LogRecord logRecord = logEvent.getLogRecord();
            String name = logEvent.getLogRecord().getLoggerName();
            org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(name);
            org.apache.log4j.Level level;
            if (logRecord.getLevel() == Level.FINEST) {
                level = org.apache.log4j.Level.DEBUG;
            } else if (logRecord.getLevel() == Level.INFO) {
                level = org.apache.log4j.Level.INFO;
            } else if (logRecord.getLevel() == Level.WARNING) {
                level = org.apache.log4j.Level.WARN;
            } else if (logRecord.getLevel() == Level.SEVERE) {
                level = org.apache.log4j.Level.FATAL;
            } else {
                level = org.apache.log4j.Level.INFO;
            }
            String message = logRecord.getMessage();
            Throwable throwable = logRecord.getThrown();
            logger.callAppenders(new LoggingEvent(name, logger, level, message, throwable));
        }
    }
}
