/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine.logging;


import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class JulLoggerFactory implements TpcLoggerFactory {

    private final ConcurrentMap<String, TpcLogger> mapLoggers = new ConcurrentHashMap<>(100);

    @Override
    public TpcLogger getLogger(String name) {
        TpcLogger logger = mapLoggers.get(name);
        if (logger != null) {
            return logger;
        }

        logger = new JulLogger(Logger.getLogger(name));
        TpcLogger found = mapLoggers.putIfAbsent(name, logger);
        return found == null ? logger : found;
    }

    private static final class JulLogger implements TpcLogger {
        private final Logger logger;

        private JulLogger(Logger logger) {
            this.logger = logger;
        }

        @Override
        public void finest(Throwable thrown) {
            log(Level.FINEST, null, thrown.getCause());
        }

        @Override
        public boolean isFinestEnabled() {
            return logger.isLoggable(Level.FINEST);
        }

        @Override
        public void fine(Throwable thrown) {
            logger.log(Level.FINE, null, thrown);
        }

        @Override
        public void fine(String message, Throwable thrown) {
            logger.log(Level.FINE, message, thrown);
        }

        @Override
        public void info(Throwable thrown) {
            logger.log(Level.INFO, null, thrown);
        }

        @Override
        public void info(String message, Throwable thrown) {
            logger.log(Level.INFO, message, thrown);
        }

        @Override
        public void warning(String message, Throwable thrown) {
            logger.log(Level.WARNING, message, thrown);
        }

        @Override
        public boolean isWarningEnabled() {
            return logger.isLoggable(Level.WARNING);
        }

        @Override
        public void severe(String message) {
            logger.severe(message);
        }

        @Override
        public boolean isSevereEnabled() {
            return logger.isLoggable(Level.SEVERE);
        }

        @Override
        public void log(Level level, String message) {
            logger.log(level, message);
        }

        @Override
        public void log(Level level, String message, Throwable thrown) {
            logger.log(level, message, thrown);
        }

        @Override
        public Level getLevel() {
            return logger.getLevel();
        }

        @Override
        public boolean isLoggable(Level level) {
            return logger.isLoggable(level);
        }

        @Override
        public boolean isFineEnabled() {
            return logger.isLoggable(Level.FINE);
        }

        @Override
        public void finest(String msg) {
            logger.finest(msg);
        }

        @Override
        public void finest(String msg, Throwable cause) {
            logger.log(Level.FINEST, cause.toString(), cause);
        }

        @Override
        public void fine(String msg) {
            logger.fine(msg);
        }

        @Override
        public boolean isInfoEnabled() {
            return logger.isLoggable(Level.INFO);
        }

        @Override
        public void info(String msg) {
            logger.info(msg);
        }

        @Override
        public void warning(Throwable cause) {
            logger.log(Level.WARNING, cause.toString(), cause);
        }

        @Override
        public void warning(String msg) {
            logger.warning(msg);
        }

        @Override
        public void severe(Throwable cause) {
            logger.log(Level.SEVERE, cause.toString(), cause);
        }

        @Override
        public void severe(String msg, Throwable cause) {
            logger.log(Level.SEVERE, cause.toString(), cause);
        }

    }
}
