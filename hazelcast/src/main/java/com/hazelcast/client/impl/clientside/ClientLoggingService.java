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

package com.hazelcast.client.impl.clientside;

import com.hazelcast.instance.BuildInfo;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.logging.AbstractLogger;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LogEvent;
import com.hazelcast.logging.LogListener;
import com.hazelcast.logging.Logger;
import com.hazelcast.logging.LoggerFactory;
import com.hazelcast.logging.LoggingService;

import javax.annotation.Nonnull;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

import static com.hazelcast.internal.util.ConcurrencyUtil.getOrPutIfAbsent;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

public class ClientLoggingService implements LoggingService {

    private final ConcurrentMap<String, ILogger> mapLoggers = new ConcurrentHashMap<>(100);

    private final ConstructorFunction<String, ILogger> loggerConstructor = DefaultLogger::new;

    private final LoggerFactory loggerFactory;
    private final BuildInfo buildInfo;
    private final String instanceName;
    private final boolean detailsEnabled;
    private volatile String versionMessage;

    public ClientLoggingService(
            String clusterName, String loggingType, BuildInfo buildInfo, String instanceName, boolean detailsEnabled
    ) {
        this.loggerFactory = Logger.newLoggerFactory(loggingType);
        this.buildInfo = buildInfo;
        this.instanceName = instanceName;
        this.detailsEnabled = detailsEnabled;
        updateClusterName(clusterName);
    }

    public void updateClusterName(String clusterName) {
        this.versionMessage = instanceName + " [" + clusterName + "] [" + buildInfo.getVersion() + "] ";
    }

    @Override
    public void addLogListener(@Nonnull Level level, @Nonnull LogListener logListener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeLogListener(@Nonnull LogListener logListener) {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    public ILogger getLogger(@Nonnull String name) {
        checkNotNull(name, "name must not be null");
        return getOrPutIfAbsent(mapLoggers, name, loggerConstructor);
    }

    @Nonnull
    public ILogger getLogger(@Nonnull Class clazz) {
        checkNotNull(clazz, "class must not be null");
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
                String logMessage = detailsEnabled ? versionMessage + message : message;
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
