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

package com.hazelcast.logging.impl;

import com.hazelcast.auditlog.AuditlogService;
import com.hazelcast.auditlog.AuditlogTypeIds;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.logging.AbstractLogger;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LogEvent;
import com.hazelcast.logging.LogListener;
import com.hazelcast.logging.Logger;
import com.hazelcast.logging.LoggerFactory;
import com.hazelcast.logging.LoggingService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;
import java.util.logging.LogRecord;

import static com.hazelcast.internal.util.ConcurrencyUtil.getOrPutIfAbsent;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.StringUtil.upperCaseInternal;

public class LoggingServiceImpl implements LoggingService {

    private final CopyOnWriteArrayList<LogListenerRegistration> listeners = new CopyOnWriteArrayList<>();

    private final ConcurrentMap<String, ILogger> mapLoggers = new ConcurrentHashMap<>(100);

    private final ConstructorFunction<String, ILogger> loggerConstructor = DefaultLogger::new;

    private final LoggerFactory loggerFactory;
    private final boolean detailsEnabled;
    private final Node node;
    private final String versionMessage;

    private volatile MemberImpl thisMember = new MemberImpl();
    private volatile String thisAddressString = "[LOCAL] ";
    private volatile Level minLevel = Level.OFF;

    private volatile Level levelSet;

    public LoggingServiceImpl(String clusterName, String loggingType, BuildInfo buildInfo, boolean detailsEnabled, Node node) {
        this.loggerFactory = Logger.newLoggerFactory(loggingType);
        this.detailsEnabled = detailsEnabled;
        this.node = node;
        versionMessage = "[" + clusterName + "] [" + buildInfo.getVersion() + "] ";
    }

    public void setThisMember(MemberImpl thisMember) {
        this.thisMember = thisMember;
        this.thisAddressString = "[" + thisMember.getAddress().getHost() + "]:"
                + thisMember.getAddress().getPort() + " ";
    }

    /**
     * @return the log level of this logging service previously set by {@link
     * #setLevel}, or {@code null} if no level was set or it was reset by {@link
     * #resetLevel}.
     */
    public @Nullable Level getLevel() {
        return levelSet;
    }

    /**
     * Sets the levels of all the loggers known to this logger service to
     * the given level. If a certain logger was already preconfigured with a more
     * verbose level than the given level, it will be kept at that more verbose
     * level.
     * <p>
     * WARNING: Keep in mind that verbose log levels like {@link Level#FINEST}
     * may severely affect the performance.
     *
     * @param level the level to set.
     * @throws HazelcastException if the underlying {@link LoggerFactory} doesn't
     *                            implement {@link InternalLoggerFactory} required
     *                            for dynamic log level changing.
     */
    public void setLevel(@Nonnull Level level) {
        if (loggerFactory instanceof InternalLoggerFactory) {
            levelSet = level;
            ((InternalLoggerFactory) loggerFactory).setLevel(level);

            AuditlogService auditlogService = node.getNodeExtension().getAuditlogService();
            auditlogService.eventBuilder(AuditlogTypeIds.MEMBER_LOGGING_LEVEL_SET).message("Log level set.").addParameter("level",
                    level).log();
        } else {
            throw new HazelcastException("Logger factory doesn't support dynamic log level changes: " + loggerFactory.getClass());
        }
    }

    /**
     * Parses the given string level into {@link Level} and then sets the level
     * using {@link #setLevel(Level)}.
     *
     * @param level the level to parse, see {@link Level#getName()} for available
     *              level names.
     * @throws IllegalArgumentException if the passed string can't be parsed into
     *                                  a known {@link Level}.
     */
    public void setLevel(@Nonnull String level) {
        Level parsedLevel;
        try {
            parsedLevel = Level.parse(upperCaseInternal(level));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Invalid level '" + level + "', known levels are: " + String.join(", ", Level.OFF.getName(),
                            Level.SEVERE.getName(), Level.WARNING.getName(), Level.INFO.getName(), Level.CONFIG.getName(),
                            Level.FINE.getName(), Level.FINER.getName(), Level.FINEST.getName()), e);
        }
        setLevel(parsedLevel);
    }

    /**
     * Resets the levels of all the loggers known to this logging service back
     * to the default reconfigured values. Basically, undoes all the changes done
     * by the previous calls to {@link #setLevel(Level)}, if there were any.
     */
    public void resetLevel() {
        if (loggerFactory instanceof InternalLoggerFactory) {
            if (levelSet != null) {
                ((InternalLoggerFactory) loggerFactory).resetLevel();
                levelSet = null;

                AuditlogService auditlogService = node.getNodeExtension().getAuditlogService();
                auditlogService.eventBuilder(AuditlogTypeIds.MEMBER_LOGGING_LEVEL_RESET).message("Log level reset.").log();
            }
        } else {
            throw new HazelcastException("Logger factory doesn't support dynamic log level changes: " + loggerFactory.getClass());
        }
    }

    @Nonnull
    @Override
    public ILogger getLogger(@Nonnull String name) {
        checkNotNull(name, "name must not be null");
        return getOrPutIfAbsent(mapLoggers, name, loggerConstructor);
    }

    @Nonnull
    @Override
    public ILogger getLogger(@Nonnull Class clazz) {
        checkNotNull(clazz, "class must not be null");
        return getOrPutIfAbsent(mapLoggers, clazz.getName(), loggerConstructor);
    }

    @Override
    public void addLogListener(@Nonnull Level level, @Nonnull LogListener logListener) {
        listeners.add(new LogListenerRegistration(level, logListener));
        if (level.intValue() < minLevel.intValue()) {
            minLevel = level;
        }
    }

    @Override
    public void removeLogListener(@Nonnull LogListener logListener) {
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

        LogListenerRegistration(@Nonnull Level level, @Nonnull LogListener logListener) {
            checkNotNull(level, "level must not be null");
            checkNotNull(logListener, "logListener must not be null");
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

    private class DefaultLogger extends AbstractLogger {
        final String name;
        final ILogger logger;

        DefaultLogger(String name) {
            this.name = name;
            this.logger = loggerFactory.getLogger(name);
        }

        @Override
        public void log(Level level, String message) {
            log(level, message, null);
        }

        @Override
        public void log(Level level, String message, Throwable thrown) {
            boolean loggable = logger.isLoggable(level);
            if (loggable || level.intValue() >= minLevel.intValue()) {
                if (detailsEnabled) {
                    String address = thisAddressString;
                    message = (address != null ? address : "") + versionMessage + message;
                }
                if (loggable) {
                    logger.log(level, message, thrown);
                }
                if (listeners.size() > 0) {
                    LogRecord logRecord = new LogRecord(level, message);
                    logRecord.setThrown(thrown);
                    logRecord.setLoggerName(name);
                    logRecord.setSourceClassName(name);
                    LogEvent logEvent = new LogEvent(logRecord, thisMember);
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
