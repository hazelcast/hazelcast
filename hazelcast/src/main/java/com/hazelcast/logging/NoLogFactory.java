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

import java.util.logging.Level;

public class NoLogFactory implements LoggerFactory {
    final ILogger noLogger = new NoLogger();

    @Override
    public ILogger getLogger(String name) {
        return noLogger;
    }

    static class NoLogger implements ILogger {
        @Override
        public void finest(String message) {
        }

        @Override
        public void finest(String message, Throwable thrown) {
        }

        @Override
        public void finest(Throwable thrown) {
        }

        @Override
        public boolean isFinestEnabled() {
            return false;
        }

        @Override
        public void info(String message) {
        }

        @Override
        public void severe(String message) {
        }

        @Override
        public void severe(Throwable thrown) {
        }

        @Override
        public void severe(String message, Throwable thrown) {
        }

        @Override
        public void warning(String message) {
        }

        @Override
        public void warning(Throwable thrown) {
        }

        @Override
        public void warning(String message, Throwable thrown) {
        }

        @Override
        public void log(Level level, String message) {
        }

        @Override
        public void log(Level level, String message, Throwable thrown) {
        }

        @Override
        public void log(LogEvent logEvent) {
        }

        @Override
        public Level getLevel() {
            return Level.OFF;
        }

        @Override
        public boolean isLoggable(Level level) {
            return false;
        }
    }
}
