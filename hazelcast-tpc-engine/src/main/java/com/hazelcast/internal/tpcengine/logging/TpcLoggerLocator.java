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

import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicReference;

public final class TpcLoggerLocator {

    private static final AtomicReference LOGGER = new AtomicReference();

    private TpcLoggerLocator() {
    }

    @SuppressWarnings("java:S112")
    public static TpcLogger getLogger(Class clazz) {
        Object logger = LOGGER.get();
        if (logger != null) {
            if (logger instanceof TpcLoggerFactory) {
                return ((TpcLoggerFactory) logger).getLogger(clazz);
            } else {
                Method method = (Method) logger;
                try {
                    return (TpcLogger) method.invoke(null, clazz);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

        try {
            Class loggerClazz = TpcLogger.class.getClassLoader().loadClass("com.hazelcast.logging.Logger");
            Method method = loggerClazz.getMethod("getLogger", Class.class);
            LOGGER.compareAndSet(null, method);
        } catch (Exception e) {
            TpcLoggerFactory loggerFactor = new JulLoggerFactory();
            LOGGER.compareAndSet(null, loggerFactor);
        }
        return getLogger(clazz);
    }

}
