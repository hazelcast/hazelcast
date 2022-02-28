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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.logging.Level;
import java.util.logging.LogRecord;

public class Slf4jFactory extends LoggerFactorySupport {

    @Override
    protected ILogger createLogger(String name) {
        Logger logger = LoggerFactory.getLogger(name);
        return new Slf4jLogger(logger);
    }

    static class Slf4jLogger extends AbstractLogger {

        private final Logger logger;

        Slf4jLogger(Logger logger) {
            this.logger = logger;
        }

        @Override
        public void log(Level level, String message) {
            if (level == Level.FINEST) {
                logger.trace(message);
            } else if (level == Level.FINER || level == Level.FINE) {
                logger.debug(message);
            } else if (level == Level.CONFIG || level == Level.INFO) {
                logger.info(message);
            } else if (level == Level.WARNING) {
                logger.warn(message);
            } else if (level == Level.SEVERE) {
                logger.error(message);
            } else if (level != Level.OFF) {
                logger.info(message);
            }
        }

        @Override
        public void log(Level level, String message, Throwable thrown) {
            if (level == Level.FINEST) {
                logger.trace(message, thrown);
            } else if (level == Level.FINER || level == Level.FINE) {
                logger.debug(message, thrown);
            } else if (level == Level.CONFIG || level == Level.INFO) {
                logger.info(message, thrown);
            } else if (level == Level.WARNING) {
                logger.warn(message, thrown);
            } else if (level == Level.SEVERE) {
                logger.error(message, thrown);
            } else if (level != Level.OFF) {
                logger.info(message, thrown);
            }
        }

        @Override
        public Level getLevel() {
            return logger.isTraceEnabled() ? Level.FINEST
                 : logger.isDebugEnabled() ? Level.FINE
                 : logger.isInfoEnabled()  ? Level.INFO
                 : logger.isWarnEnabled()  ? Level.WARNING
                 : logger.isErrorEnabled() ? Level.SEVERE
                 : Level.OFF;
        }

        @Override
        public boolean isLoggable(Level level) {
            return level == Level.FINEST  ? logger.isTraceEnabled()
                 : level == Level.FINER   ? logger.isDebugEnabled()
                 : level == Level.FINE    ? logger.isDebugEnabled()
                 : level == Level.CONFIG  ? logger.isInfoEnabled()
                 : level == Level.INFO    ? logger.isInfoEnabled()
                 : level == Level.WARNING ? logger.isWarnEnabled()
                 : level == Level.SEVERE  ? logger.isErrorEnabled()
                 : level != Level.OFF && logger.isInfoEnabled();
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
