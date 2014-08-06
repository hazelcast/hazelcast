/*
 * Copyright (c) 2014, Appear Networks. All Rights Reserved.
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.spi.ExtendedLogger;

import java.util.logging.Level;
import java.util.logging.LogRecord;

/**
 * Logging to Log4j 2.x.
 */
public class Log4j2Factory extends LoggerFactorySupport {

    private static final String FQCN = Log4j2Logger.class.getName();

    protected ILogger createLogger(String name) {
        return new Log4j2Logger(LogManager.getContext().getLogger(name));
    }

    class Log4j2Logger extends AbstractLogger {
        private final ExtendedLogger logger;

        public Log4j2Logger(ExtendedLogger logger) {
            this.logger = logger;
        }

        @Override
        public void log(LogEvent logEvent) {
            LogRecord logRecord = logEvent.getLogRecord();
            Level level = logEvent.getLogRecord().getLevel();
            String message = logRecord.getMessage();
            Throwable thrown = logRecord.getThrown();
            log(level, message, thrown);
        }

        @Override
        public void log(Level level, String message) {
            logger.logIfEnabled(FQCN, getLevel(level), null, message);
        }

        @Override
        public void log(Level level, String message, Throwable thrown) {
            logger.logIfEnabled(FQCN, getLevel(level), null, message, thrown);
        }

        @Override
        public Level getLevel() {
            if (logger.isDebugEnabled()) {
                return Level.FINEST;
            } else if (logger.isInfoEnabled()) {
                return Level.INFO;
            } else if (logger.isWarnEnabled()) {
                return Level.WARNING;
            } else {
                return Level.SEVERE;
            }
        }

        @Override
        public boolean isLoggable(Level level) {
            if (Level.OFF == level) {
                return false;
            } else {
                return logger.isEnabled(getLevel(level), null);
            }
        }

        private org.apache.logging.log4j.Level getLevel(Level level) {
            if (Level.SEVERE == level) {
                return org.apache.logging.log4j.Level.ERROR;
            } else if (Level.WARNING == level) {
                return org.apache.logging.log4j.Level.WARN;
            } else if (Level.INFO == level) {
                return org.apache.logging.log4j.Level.INFO;
            } else if (Level.CONFIG == level) {
                return org.apache.logging.log4j.Level.INFO;
            } else if (Level.FINE == level) {
                return org.apache.logging.log4j.Level.DEBUG;
            } else if (Level.FINER == level) {
                return org.apache.logging.log4j.Level.DEBUG;
            } else if (Level.FINEST == level) {
                return org.apache.logging.log4j.Level.DEBUG;
            } else {
                return org.apache.logging.log4j.Level.INFO;
            }
        }
    }
}
