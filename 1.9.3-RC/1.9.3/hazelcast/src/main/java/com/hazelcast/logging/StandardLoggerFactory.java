/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.logging;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

public class StandardLoggerFactory implements LoggerFactory {
    final ConcurrentMap<String, ILogger> mapLoggers = new ConcurrentHashMap<String, ILogger>(100);

    public ILogger getLogger(String name) {
        ILogger logger = mapLoggers.get(name);
        if (logger == null) {
            Logger l = Logger.getLogger(name);
            ILogger newLogger = new StandardLogger(l);
            logger = mapLoggers.putIfAbsent(name, newLogger);
            if (logger == null) {
                logger = newLogger;
            }
        }
        return logger;
    }

    class StandardLogger implements ILogger {
        private final Logger logger;

        public StandardLogger(Logger logger) {
            this.logger = logger;
        }

        public void log(Level level, String message) {
            log(level, message, null);
        }

        public void log(Level level, String message, Throwable thrown) {
            LogRecord logRecord = new LogRecord(level, message);
            logRecord.setLoggerName(logger.getName());
            logRecord.setThrown(thrown);
            logRecord.setSourceClassName(logger.getName());
            logger.log(logRecord);
        }

        public void log(LogEvent logEvent) {
            logger.log(logEvent.getLogRecord());
        }

        public Level getLevel() {
            return logger.getLevel();
        }

        public boolean isLoggable(Level level) {
            return logger.isLoggable(level);
        }
    }
}
