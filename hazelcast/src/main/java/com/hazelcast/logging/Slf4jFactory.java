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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.logging.Level;
import java.util.logging.LogRecord;

public class Slf4jFactory extends LoggerFactorySupport {

    @Override
    protected ILogger createLogger(String name) {
        final Logger l = LoggerFactory.getLogger(name);
        return new Slf4jLogger(l);
    }

    static class Slf4jLogger extends AbstractLogger {
        private final Logger logger;

        public Slf4jLogger(Logger logger) {
            this.logger = logger;
        }

        @Override
        public void log(Level level, String message) {
            if (Level.FINEST == level) {
                logger.debug(message);
            } else if (Level.SEVERE == level) {
                logger.error(message);
            } else if (Level.WARNING == level) {
                logger.warn(message);
            } else {
                logger.info(message);
            }
        }

        @Override
        public Level getLevel() {
            if (logger.isErrorEnabled()) {
                return Level.SEVERE;
            } else if (logger.isWarnEnabled()) {
                return Level.WARNING;
            } else if (logger.isInfoEnabled()) {
                return Level.INFO;
            } else {
                return Level.FINEST;
            }
        }

        @Override
        public boolean isLoggable(Level level) {
            if (Level.OFF == level) {
                return false;
            } else if (Level.FINEST == level) {
                return logger.isDebugEnabled();
            } else if (Level.INFO == level) {
                return logger.isInfoEnabled();
            } else if (Level.WARNING == level) {
                return logger.isWarnEnabled();
            } else if (Level.SEVERE == level) {
                return logger.isErrorEnabled();
            } else {
                return logger.isInfoEnabled();
            }
        }

        @Override
        public void log(Level level, String message, Throwable thrown) {
            if (Level.FINEST == level) {
                logger.debug(message, thrown);
            } else if (Level.INFO == level) {
                logger.info(message, thrown);
            } else if (Level.WARNING == level) {
                logger.warn(message, thrown);
            } else if (Level.SEVERE == level) {
                logger.error(message, thrown);
            } else {
                logger.info(message, thrown);
            }
        }

        @Override
        public void log(LogEvent logEvent) {
            LogRecord logRecord = logEvent.getLogRecord();
            Level level = logEvent.getLogRecord().getLevel();
            String message = logRecord.getMessage();
            Throwable thrown = logRecord.getThrown();
            log(level, message, thrown);
        }
    }
}
