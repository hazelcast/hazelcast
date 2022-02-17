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

package com.hazelcast.test;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Log4j2Factory;
import com.hazelcast.logging.LogEvent;
import com.hazelcast.logging.LoggerFactorySupport;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.spi.LoggerContext;

import java.net.URI;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * The factory uses Log4j2 internally, but its loggers always return
 * {@code true} to guards such as {@code isFinestEnabled()} etc.
 * <p>
 * The real filtering is happening in the log4js once again. Thus it
 * covers branches guarded by is-level-enabled checks yet the real
 * logging is configurable via {@code log4j2.xml}.
 * <p>
 * It also supports changing logging configuration on-the-fly, see
 * {@link TestLoggerFactory#changeConfigFile}.
 */
public class TestLoggerFactory extends LoggerFactorySupport {

    /**
     * Log4j XML configuration being currently used, {@code null}
     * indicates the Log4j default behavior.
     */
    private URI configFile = null;

    /**
     * Store all the logging context being created, because we need to
     * clear them in order to change the configuration to reload it.
     */
    private final Collection<LoggerContext> loggerContexts = new ConcurrentLinkedQueue<LoggerContext>();

    /**
     * Reference to a {@link Log4j2Factory}, which is used to create
     * loggers for older Hazelcast versions.
     */
    private final AtomicReference<Log4j2Factory> legacyLog4j2Factory = new AtomicReference<Log4j2Factory>();

    /**
     * Changes the configuration to be used.
     *
     * @param configName the Log4j XML configuration to be used,
     *                   {@code null} for default behavior
     */
    public void changeConfigFile(String configName) {
        for (LoggerContext context : loggerContexts) {
            LogManager.getFactory().removeContext(context);
        }

        configFile = configName != null ? URI.create(configName) : null;
        clearLoadedLoggers();
        loggerContexts.clear();
    }

    protected ILogger createLogger(String name) {
        LoggerContext loggerContext = LogManager.getContext(null, false, configFile);
        loggerContexts.add(loggerContext);

        ILogger delegate;
        try {
            delegate = new Log4j2Factory.Log4j2Logger(loggerContext.getLogger(name));
        } catch (IllegalAccessError e) {
            // older Hazelcast versions cannot access Log4j2Logger, so we fallback to the legacy code
            delegate = getOrCreateLegacyLog4j2Factory().getLogger(name);
        }
        return new DelegatingTestLogger(delegate);
    }

    private Log4j2Factory getOrCreateLegacyLog4j2Factory() {
        Log4j2Factory factory = legacyLog4j2Factory.get();
        if (factory != null) {
            return factory;
        }
        Log4j2Factory candidate = new Log4j2Factory();
        if (legacyLog4j2Factory.compareAndSet(null, candidate)) {
            return candidate;
        }
        return legacyLog4j2Factory.get();
    }

    private static class DelegatingTestLogger implements ILogger {

        private static final long WARNING_THRESHOLD_NANOS = MILLISECONDS.toNanos(500);

        private ILogger delegate;

        private DelegatingTestLogger(ILogger delegate) {
            this.delegate = delegate;
        }

        @Override
        public void finest(String message) {
            long startNanos = System.nanoTime();
            delegate.finest(message);
            logOnSlowLogging(startNanos);
        }

        @Override
        public void finest(Throwable thrown) {
            long startNanos = System.nanoTime();
            delegate.finest(thrown);
            logOnSlowLogging(startNanos);
        }

        @Override
        public void finest(String message, Throwable thrown) {
            long startNanos = System.nanoTime();
            delegate.finest(message, thrown);
            logOnSlowLogging(startNanos);
        }

        @Override
        public boolean isFinestEnabled() {
            return true;
        }

        @Override
        public void fine(String message) {
            long startNanos = System.nanoTime();
            delegate.fine(message);
            logOnSlowLogging(startNanos);
        }

        @Override
        public void fine(Throwable thrown) {
            long startNanos = System.nanoTime();
            delegate.fine(thrown);
            logOnSlowLogging(startNanos);
        }

        @Override
        public void fine(String message, Throwable thrown) {
            long startNanos = System.nanoTime();
            delegate.fine(message, thrown);
            logOnSlowLogging(startNanos);
        }

        @Override
        public boolean isFineEnabled() {
            return true;
        }

        @Override
        public void info(String message) {
            long startNanos = System.nanoTime();
            delegate.info(message);
            logOnSlowLogging(startNanos);
        }

        @Override
        public void info(String message, Throwable thrown) {
            long startNanos = System.nanoTime();
            delegate.info(message, thrown);
            logOnSlowLogging(startNanos);
        }

        @Override
        public void info(Throwable thrown) {
            long startNanos = System.nanoTime();
            delegate.info(thrown);
            logOnSlowLogging(startNanos);
        }

        @Override
        public boolean isInfoEnabled() {
            return true;
        }

        @Override
        public void warning(String message) {
            long startNanos = System.nanoTime();
            delegate.warning(message);
            logOnSlowLogging(startNanos);
        }

        @Override
        public void warning(Throwable thrown) {
            long startNanos = System.nanoTime();
            delegate.warning(thrown);
            logOnSlowLogging(startNanos);
        }

        @Override
        public void warning(String message, Throwable thrown) {
            long startNanos = System.nanoTime();
            delegate.warning(message, thrown);
            logOnSlowLogging(startNanos);
        }

        @Override
        public boolean isWarningEnabled() {
            return true;
        }

        @Override
        public void severe(String message) {
            long startNanos = System.nanoTime();
            delegate.severe(message);
            logOnSlowLogging(startNanos);
        }

        @Override
        public void severe(Throwable thrown) {
            long startNanos = System.nanoTime();
            delegate.severe(thrown);
            logOnSlowLogging(startNanos);
        }

        @Override
        public void severe(String message, Throwable thrown) {
            long startNanos = System.nanoTime();
            delegate.severe(message, thrown);
            logOnSlowLogging(startNanos);
        }

        @Override
        public boolean isSevereEnabled() {
            return true;
        }

        @Override
        public void log(Level level, String message) {
            long startNanos = System.nanoTime();
            delegate.log(level, message);
            logOnSlowLogging(startNanos);
        }

        @Override
        public void log(Level level, String message, Throwable thrown) {
            long startNanos = System.nanoTime();
            delegate.log(level, message, thrown);
            logOnSlowLogging(startNanos);
        }

        @Override
        public void log(LogEvent logEvent) {
            long startNanos = System.nanoTime();
            delegate.log(logEvent);
            logOnSlowLogging(startNanos);
        }

        @Override
        public Level getLevel() {
            return Level.ALL;
        }

        @Override
        public boolean isLoggable(Level level) {
            return true;
        }

        private void logOnSlowLogging(long startTimeNanos) {
            long durationNanos = System.nanoTime() - startTimeNanos;
            if (durationNanos > WARNING_THRESHOLD_NANOS) {
                long durationMillis = NANOSECONDS.toMillis(durationNanos);
                delegate.warning("Logging took " + durationMillis + " ms.");
            }
        }
    }
}
