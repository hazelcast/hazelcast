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

import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.util.ConstructorFunction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;
import java.util.logging.LogRecord;

import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;

public class LoggingServiceImpl implements LoggingService {

    private volatile MemberImpl thisMember = new MemberImpl();
    private final String groupName;
    private final CopyOnWriteArrayList<LogListenerRegistration> listeners
            = new CopyOnWriteArrayList<LogListenerRegistration>();
    private volatile String thisAddressString = "[LOCAL]";

    private final ConcurrentMap<String, ILogger> mapLoggers = new ConcurrentHashMap<String, ILogger>(100);

    private final ConstructorFunction<String, ILogger> loggerConstructor
            = new ConstructorFunction<String, ILogger>() {

        @Override
        public ILogger createNew(String key) {
            return new DefaultLogger(key);
        }
    };

    private final LoggerFactory loggerFactory;
    private final BuildInfo buildInfo;
    private volatile Level minLevel = Level.OFF;

    public LoggingServiceImpl(String groupName, String loggingType, BuildInfo buildInfo) {
        this.groupName = groupName;
        this.loggerFactory = Logger.newLoggerFactory(loggingType);
        this.buildInfo = buildInfo;
    }

    public void setThisMember(MemberImpl thisMember) {
        this.thisMember = thisMember;
        this.thisAddressString = "[" + thisMember.getAddress().getHost() + "]:" + thisMember.getAddress().getPort();
    }

    @Override
    public ILogger getLogger(String name) {
        return getOrPutIfAbsent(mapLoggers, name, loggerConstructor);
    }

    @Override
    public ILogger getLogger(Class clazz) {
        return getOrPutIfAbsent(mapLoggers, clazz.getName(), loggerConstructor);
    }

    @Override
    public void addLogListener(Level level, LogListener logListener) {
        listeners.add(new LogListenerRegistration(level, logListener));
        if (level.intValue() < minLevel.intValue()) {
            minLevel = level;
        }
    }

    @Override
    public void removeLogListener(LogListener logListener) {
        listeners.remove(new LogListenerRegistration(Level.ALL, logListener));
    }

    void handleLogEvent(LogEvent logEvent) {
        for (LogListenerRegistration logListenerRegistration : listeners) {
            if (logEvent.getLogRecord().getLevel().intValue() >= logListenerRegistration.getLevel().intValue()) {
                logListenerRegistration.getLogListener().log(logEvent);
            }
        }
    }

    private static class LogListenerRegistration {
        final Level level;
        final LogListener logListener;

        LogListenerRegistration(Level level, LogListener logListener) {
            this.level = level;
            this.logListener = logListener;
        }

        public Level getLevel() {
            return level;
        }

        public LogListener getLogListener() {
            return logListener;
        }

        /**
         * True if LogListeners are equal.
         */
        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            LogListenerRegistration other = (LogListenerRegistration) obj;
            if (logListener == null) {
                if (other.logListener != null) {
                    return false;
                }
            } else if (!logListener.equals(other.logListener)) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            return logListener != null ? logListener.hashCode() : 0;
        }
    }

    private class DefaultLogger implements ILogger {
        final String name;
        final ILogger logger;

        DefaultLogger(String name) {
            this.name = name;
            this.logger = loggerFactory.getLogger(name);
        }

        @Override
        public void finest(String message) {
            log(Level.FINEST, message);
        }

        @Override
        public void finest(String message, Throwable thrown) {
            log(Level.FINEST, message, thrown);
        }

        @Override
        public void finest(Throwable thrown) {
            log(Level.FINEST, thrown.getMessage(), thrown);
        }

        @Override
        public boolean isFinestEnabled() {
            return isLoggable(Level.FINEST);
        }

        @Override
        public void info(String message) {
            log(Level.INFO, message);
        }

        @Override
        public void severe(String message) {
            log(Level.SEVERE, message);
        }

        @Override
        public void severe(Throwable thrown) {
            log(Level.SEVERE, thrown.getMessage(), thrown);
        }

        @Override
        public void severe(String message, Throwable thrown) {
            log(Level.SEVERE, message, thrown);
        }

        @Override
        public void warning(String message) {
            log(Level.WARNING, message);
        }

        @Override
        public void warning(Throwable thrown) {
            log(Level.WARNING, thrown.getMessage(), thrown);
        }

        @Override
        public void warning(String message, Throwable thrown) {
            log(Level.WARNING, message, thrown);
        }

        @Override
        public void log(Level level, String message) {
            log(level, message, null);
        }

        @Override
        public void log(Level level, String message, Throwable thrown) {
            boolean loggable = logger.isLoggable(level);
            if (loggable || level.intValue() >= minLevel.intValue()) {
                String logRecordMessage = thisAddressString + " [" + groupName + "] "
                        + "[" + buildInfo.getVersion() + "] " + message;
                LogRecord logRecord = new LogRecord(level, logRecordMessage);
                logRecord.setThrown(thrown);
                logRecord.setLoggerName(name);
                logRecord.setSourceClassName(name);
                LogEvent logEvent = new LogEvent(logRecord, groupName, thisMember);
                if (loggable) {
                    logger.log(logEvent);
                }
                if (listeners.size() > 0) {
                    handleLogEvent(logEvent);
                }
            }
        }

        @Override
        public void log(LogEvent logEvent) {
            handleLogEvent(logEvent);
        }

        @Override
        public Level getLevel() {
            return logger.getLevel();
        }

        @Override
        public boolean isLoggable(Level level) {
            return logger.isLoggable(level);
        }
    }
}
