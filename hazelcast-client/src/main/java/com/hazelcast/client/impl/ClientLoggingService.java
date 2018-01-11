/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl;

import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.JetBuildInfo;
import com.hazelcast.logging.AbstractLogger;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LogEvent;
import com.hazelcast.logging.LogListener;
import com.hazelcast.logging.Logger;
import com.hazelcast.logging.LoggerFactory;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.util.ConstructorFunction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;

public class ClientLoggingService implements LoggingService {

    private final ConcurrentMap<String, ILogger> mapLoggers = new ConcurrentHashMap<String, ILogger>(100);

    private final ConstructorFunction<String, ILogger> loggerConstructor
            = new ConstructorFunction<String, ILogger>() {

        @Override
        public ILogger createNew(String key) {
            return new DefaultLogger(key);
        }
    };

    private final LoggerFactory loggerFactory;
    private final String versionMessage;

    public ClientLoggingService(String groupName, String loggingType, BuildInfo buildInfo, String clientName) {
        this.loggerFactory = Logger.newLoggerFactory(loggingType);
        JetBuildInfo jetBuildInfo = buildInfo.getJetBuildInfo();
        this.versionMessage = clientName + " [" + groupName + "]"
                + (jetBuildInfo != null ? " [" + jetBuildInfo.getVersion() + "]" : "")
                + " [" + buildInfo.getVersion() + "] ";
    }

    @Override
    public void addLogListener(Level level, LogListener logListener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeLogListener(LogListener logListener) {
        throw new UnsupportedOperationException();
    }

    public ILogger getLogger(String name) {
        return getOrPutIfAbsent(mapLoggers, name, loggerConstructor);
    }

    public ILogger getLogger(Class clazz) {
        return getOrPutIfAbsent(mapLoggers, clazz.getName(), loggerConstructor);
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
            if (logger.isLoggable(level)) {
                String logMessage = versionMessage + message;
                logger.log(level, logMessage, thrown);
            }
        }

        @Override
        public void log(LogEvent logEvent) {
            // unused
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
