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

import com.hazelcast.cluster.ClusterServiceImpl;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;
import java.util.logging.LogRecord;

public class LoggingServiceImpl implements LoggingService {
    private final MemberImpl thisMember;
    private final SystemLogService systemLogService;
    private final String groupName;
    private final CopyOnWriteArrayList<LogListenerRegistration> lsListeners
            = new CopyOnWriteArrayList<LogListenerRegistration>();

    private final String thisAddressString;

    private final ConcurrentMap<String, ILogger> mapLoggers = new ConcurrentHashMap<String, ILogger>(100);
    private final LoggerFactory loggerFactory;
    private volatile Level minLevel = Level.OFF;

    public LoggingServiceImpl(SystemLogService systemLogService, String groupName, String loggingType, MemberImpl thisMember) {
        this.systemLogService = systemLogService;
        this.groupName = groupName;
        this.thisMember = thisMember;
        this.loggerFactory = Logger.newLoggerFactory(loggingType);
        thisAddressString = "[" + thisMember.getAddress().getHost() + "]:" + thisMember.getAddress().getPort();
    }

    final ConstructorFunction<String, ILogger> loggerConstructor
            = new ConstructorFunction<String, ILogger>() {
        public ILogger createNew(String key) {
            return new DefaultLogger(key);
        }
    };

    public ILogger getLogger(String name) {
        return ConcurrencyUtil.getOrPutIfAbsent(mapLoggers, name, loggerConstructor);
    }

    public ILogger getLogger(Class clazz) {
        return ConcurrencyUtil.getOrPutIfAbsent(mapLoggers, clazz.getName(), loggerConstructor);
    }

    public void addLogListener(Level level, LogListener logListener) {
        lsListeners.add(new LogListenerRegistration(level, logListener));
        if (level.intValue() < minLevel.intValue()) {
            minLevel = level;
        }
    }

    public void removeLogListener(LogListener logListener) {
        lsListeners.remove(new LogListenerRegistration(Level.ALL, logListener));
    }

    void handleLogEvent(LogEvent logEvent) {
        for (LogListenerRegistration logListenerRegistration : lsListeners) {
            if (logEvent.getLogRecord().getLevel().intValue() >= logListenerRegistration.getLevel().intValue()) {
                logListenerRegistration.getLogListener().log(logEvent);
            }
        }
    }

    class LogListenerRegistration {
        Level level;
        LogListener logListener;

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
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            LogListenerRegistration other = (LogListenerRegistration) obj;
            if (logListener == null) {
                if (other.logListener != null)
                    return false;
            } else if (!logListener.equals(other.logListener))
                return false;
            return true;
        }

        @Override
        public int hashCode() {
            return logListener != null ? logListener.hashCode() : 0;
        }
    }

    class DefaultLogger implements ILogger {
        final String name;
        final ILogger logger;
        final boolean addToLoggingService;

        DefaultLogger(String name) {
            this.name = name;
            this.logger = loggerFactory.getLogger(name);
            addToLoggingService = (name.equals(ClusterServiceImpl.class.getName()));
        }

        public void log(Level level, String message) {
            log(level, message, null);
        }

        public void log(Level level, String message, Throwable thrown) {
            if (addToLoggingService) {
                systemLogService.logNode(message + ((thrown == null) ? "" : ": " + thrown.getMessage()));
            }
            boolean loggable = logger.isLoggable(level);
            if (loggable || level.intValue() >= minLevel.intValue()) {
                message = thisAddressString + " [" + groupName + "] " + message;
                LogRecord logRecord = new LogRecord(level, message);
                logRecord.setThrown(thrown);
                logRecord.setLoggerName(name);
                logRecord.setSourceClassName(name);
                LogEvent logEvent = new LogEvent(logRecord, groupName, thisMember);
                if (loggable) {
                    logger.log(logEvent);
                }
                if (lsListeners.size() > 0) {
                    handleLogEvent(logEvent);
                }
            }
        }

        public void log(LogEvent logEvent) {
            handleLogEvent(logEvent);
        }

        public Level getLevel() {
            return logger.getLevel();
        }

        public boolean isLoggable(Level level) {
            return logger.isLoggable(level);
        }
    }
}
