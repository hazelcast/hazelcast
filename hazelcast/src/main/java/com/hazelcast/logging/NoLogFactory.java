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

    public ILogger getLogger(String name) {
        return noLogger;
    }

    class NoLogger implements ILogger {
        public void finest(String message) {
        }

        public void finest(String message, Throwable thrown) {
        }

        public void finest(Throwable thrown) {
        }

        public boolean isFinestEnabled() {
           return false;
        }

        public void info(String message) {
        }

        public void severe(String message) {
        }

        public void severe(Throwable thrown) {
        }

        public void severe(String message, Throwable thrown) {
        }

        public void warning(String message) {
        }

        public void warning(Throwable thrown) {
        }

        public void warning(String message, Throwable thrown) {
        }

        public void log(Level level, String message) {
        }

        public void log(Level level, String message, Throwable thrown) {
        }

        public void log(LogEvent logEvent) {
        }

        public Level getLevel() {
            return Level.OFF;
        }

        public boolean isLoggable(Level level) {
            return false;
        }
    }
}
