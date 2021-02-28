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

import com.hazelcast.logging.impl.InternalLogger;
import com.hazelcast.spi.annotation.PrivateApi;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.spi.ExtendedLogger;

import java.util.logging.Level;
import java.util.logging.LogRecord;

/**
 * Logging to Log4j 2.x.
 */
public class Log4j2Factory extends LoggerFactorySupport {

    private static final String FQCN = Log4j2Logger.class.getName();

    protected ILogger createLogger(String name) {
        return new Log4j2Logger(LogManager.getContext(false).getLogger(name));
    }

    @PrivateApi
    public static class Log4j2Logger extends AbstractLogger implements InternalLogger {

        private final ExtendedLogger logger;

        public Log4j2Logger(ExtendedLogger logger) {
            this.logger = logger;
        }

        @Override
        public void setLevel(Level level) {
            Configurator.setLevel(logger.getName(), toLog4j2Level(level));
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
            logger.logIfEnabled(FQCN, toLog4j2Level(level), null, message);
        }

        @Override
        public void log(Level level, String message, Throwable thrown) {
            logger.logIfEnabled(FQCN, toLog4j2Level(level), null, message, thrown);
        }

        @Override
        public Level getLevel() {
            return logger.isTraceEnabled() ? Level.FINEST
                 : logger.isDebugEnabled() ? Level.FINE
                 : logger.isInfoEnabled()  ? Level.INFO
                 : logger.isWarnEnabled()  ? Level.WARNING
                 : logger.isErrorEnabled() ? Level.SEVERE
                 : logger.isFatalEnabled() ? Level.SEVERE
                 : Level.OFF;
        }

        @Override
        public boolean isLoggable(Level level) {
            return level != Level.OFF && logger.isEnabled(toLog4j2Level(level), null);
        }

        private static org.apache.logging.log4j.Level toLog4j2Level(Level level) {
            return level == Level.FINEST  ? org.apache.logging.log4j.Level.TRACE
                 : level == Level.FINE    ? org.apache.logging.log4j.Level.DEBUG
                 : level == Level.INFO    ? org.apache.logging.log4j.Level.INFO
                 : level == Level.WARNING ? org.apache.logging.log4j.Level.WARN
                 : level == Level.SEVERE  ? org.apache.logging.log4j.Level.ERROR
                 : level == Level.FINER   ? org.apache.logging.log4j.Level.DEBUG
                 : level == Level.CONFIG  ? org.apache.logging.log4j.Level.INFO
                 : level == Level.OFF     ? org.apache.logging.log4j.Level.OFF
                 : org.apache.logging.log4j.Level.INFO;
        }
    }
}
