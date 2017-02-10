package com.hazelcast.test;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Log4j2Factory;
import com.hazelcast.logging.LogEvent;
import com.hazelcast.logging.LoggerFactory;

import java.util.logging.Level;

/**
 * The factory uses log4j2 internally, however loggers always
 * return true to guards such as `isFinestEnabled()` etc.
 *
 * The real filtering is happening in the log4js once again.
 * Thus it covers branches guarded by is-level-enabled checks
 * yet the real logging is configurable via log4j2.xml
 *
 */
public class TestLoggerFactory implements LoggerFactory {

    private Log4j2Factory log4j2Factory = new Log4j2Factory();

    @Override
    public ILogger getLogger(String name) {
        ILogger logger = log4j2Factory.getLogger(name);
        return new DelegatingTestLogger(logger);
    }

    private static class DelegatingTestLogger implements ILogger {

        private ILogger delegate;

        private DelegatingTestLogger(ILogger delegate) {
            this.delegate = delegate;
        }

        @Override
        public void finest(String message) {
            delegate.finest(message);
        }

        @Override
        public void finest(Throwable thrown) {
            delegate.finest(thrown);
        }

        @Override
        public void finest(String message, Throwable thrown) {
            delegate.finest(message, thrown);
        }

        @Override
        public boolean isFinestEnabled() {
            return true;
        }

        @Override
        public void fine(String message) {
            delegate.fine(message);
        }

        @Override
        public void fine(Throwable thrown) {
            delegate.fine(thrown);
        }

        @Override
        public void fine(String message, Throwable thrown) {
            delegate.fine(message, thrown);
        }

        @Override
        public boolean isFineEnabled() {
            return true;
        }

        @Override
        public void info(String message) {
            delegate.info(message);
        }

        @Override
        public boolean isInfoEnabled() {
            return true;
        }

        @Override
        public void warning(String message) {
            delegate.warning(message);
        }

        @Override
        public void warning(Throwable thrown) {
            delegate.warning(thrown);
        }

        @Override
        public void warning(String message, Throwable thrown) {
            delegate.warning(message, thrown);
        }

        @Override
        public boolean isWarningEnabled() {
            return true;
        }

        @Override
        public void severe(String message) {
            delegate.severe(message);
        }

        @Override
        public void severe(Throwable thrown) {
            delegate.severe(thrown);
        }

        @Override
        public void severe(String message, Throwable thrown) {
            delegate.severe(message, thrown);
        }

        @Override
        public void log(Level level, String message) {
            delegate.log(level, message);
        }

        @Override
        public void log(Level level, String message, Throwable thrown) {
            delegate.log(level, message, thrown);
        }

        @Override
        public void log(LogEvent logEvent) {
            delegate.log(logEvent);
        }

        @Override
        public Level getLevel() {
            return Level.ALL;
        }

        @Override
        public boolean isLoggable(Level level) {
            return true;
        }
    }
}
