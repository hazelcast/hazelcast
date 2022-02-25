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

package com.hazelcast.logging;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.util.ConcurrencyUtil;
import com.hazelcast.internal.util.ConcurrentReferenceHashMap;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.logging.impl.InternalLogger;
import com.hazelcast.logging.impl.InternalLoggerFactory;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

public abstract class LoggerFactorySupport implements LoggerFactory, InternalLoggerFactory {

    final ConcurrentMap<String, ILogger> mapLoggers = new ConcurrentHashMap<>(100);
    final ConstructorFunction<String, ILogger> loggerConstructor = this::createLogger;
    final ConcurrentMap<ILogger, Optional<Level>> levels = new ConcurrentReferenceHashMap<>(100);

    @Override
    public final ILogger getLogger(String name) {
        return ConcurrencyUtil.getOrPutIfAbsent(mapLoggers, name, loggerConstructor);
    }

    protected abstract ILogger createLogger(String name);

    public void clearLoadedLoggers() {
        mapLoggers.clear();
        levels.clear();
    }

    @SuppressWarnings("OptionalAssignedToNull")
    @Override
    public void setLevel(@Nonnull Level level) {
        for (ILogger logger : mapLoggers.values()) {
            Optional<Level> currentLevel = levels.computeIfAbsent(logger,
                    l -> level == Level.OFF || l.isLoggable(level) ? null : Optional.ofNullable(l.getLevel()));

            if (currentLevel != null) {
                obtainInternalLogger(logger).setLevel(level);
            }
        }
    }

    @Override
    public void resetLevel() {
        Iterator<Map.Entry<ILogger, Optional<Level>>> iterator = levels.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<ILogger, Optional<Level>> entry = iterator.next();

            ILogger logger = entry.getKey();
            Level level = entry.getValue().orElse(null);
            obtainInternalLogger(logger).setLevel(level);

            iterator.remove();
        }
    }

    private InternalLogger obtainInternalLogger(ILogger logger) {
        if (logger instanceof InternalLogger) {
            return (InternalLogger) logger;
        } else {
            throw new HazelcastException("Logger doesn't support dynamic log level changes: " + logger.getClass());
        }
    }

}
