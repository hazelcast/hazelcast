/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

public abstract class LoggerFactorySupport implements LoggerFactory {

    final ConcurrentMap<String, ILogger> mapLoggers = new ConcurrentHashMap<>(100);

    final Function<String, ILogger> loggerConstructor = this::createLogger;

    @Override
    public final ILogger getLogger(String name) {
        return mapLoggers.computeIfAbsent(name, loggerConstructor);
    }

    protected abstract ILogger createLogger(String name);

    public void clearLoadedLoggers() {
        mapLoggers.clear();
    }
}
