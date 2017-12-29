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

package com.hazelcast.logging;

import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.util.StringUtil;

/**
 * Provides static utilities to access the global shared logging machinery.
 * <p>
 * Note: if possible, avoid logging in the global shared context exposed by the utilities of this class,
 * use {@link LoggingService} instead.
 */
public final class Logger {

    private static volatile LoggerFactory loggerFactory;

    private static final Object FACTORY_LOCK = new Object();

    private Logger() {
    }

    /**
     * Obtains a {@link ILogger logger} for the given {@code clazz}.
     *
     * @param clazz the class to obtain the logger for.
     * @return the obtained logger.
     */
    public static ILogger getLogger(Class clazz) {
        return getLogger(clazz.getName());
    }

    /**
     * Obtains a {@link ILogger logger} of the given {@code name}.
     *
     * @param name the name of the logger to obtain.
     * @return the obtained logger.
     */
    public static ILogger getLogger(String name) {
        return getOrCreateLoggerFactory(System.getProperty("hazelcast.logging.type")).getLogger(name);
    }

    /**
     * @return the no operation logger, which ignores all logging requests.
     */
    public static ILogger noLogger() {
        return new NoLogFactory.NoLogger();
    }

    /**
     * Obtains a {@link LoggerFactory logger factory} instance of the given preferred type.
     * <p>
     * Despite its name this method doesn't necessarily return a new instance of logger factory. Also, the type of
     * the obtained logger factory doesn't necessarily match the given preferred type. For example, if the global
     * shared logging machinery is already initialized with a different factory type, the existing shared logger
     * factory instance will be returned.
     *
     * @param preferredType the preferred type of the logger factory to obtain.
     * @return the obtained logger factory.
     */
    public static LoggerFactory newLoggerFactory(String preferredType) {
        return getOrCreateLoggerFactory(preferredType);
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    private static LoggerFactory getOrCreateLoggerFactory(String preferredType) {
        LoggerFactory existingFactory = loggerFactory;
        if (existingFactory != null) {
            return existingFactory;
        }

        synchronized (FACTORY_LOCK) {
            existingFactory = loggerFactory;
            if (existingFactory != null) {
                return existingFactory;
            }

            LoggerFactory createdFactory = null;

            final String factoryClass = System.getProperty("hazelcast.logging.class");
            if (!StringUtil.isNullOrEmpty(factoryClass)) {
                createdFactory = tryCreateLoggerFactory(factoryClass);
            }

            if (createdFactory == null && preferredType != null) {
                if ("log4j".equals(preferredType)) {
                    createdFactory = tryCreateLoggerFactory("com.hazelcast.logging.Log4jFactory");
                } else if ("log4j2".equals(preferredType)) {
                    createdFactory = tryCreateLoggerFactory("com.hazelcast.logging.Log4j2Factory");
                } else if ("slf4j".equals(preferredType)) {
                    createdFactory = tryCreateLoggerFactory("com.hazelcast.logging.Slf4jFactory");
                } else if ("jdk".equals(preferredType)) {
                    createdFactory = new StandardLoggerFactory();
                } else if ("none".equals(preferredType)) {
                    createdFactory = new NoLogFactory();
                }
            }

            if (createdFactory == null) {
                createdFactory = new StandardLoggerFactory();
            }

            loggerFactory = createdFactory;
            return createdFactory;
        }
    }

    private static LoggerFactory tryCreateLoggerFactory(String className) {
        try {
            return ClassLoaderUtil.newInstance(null, className);
        } catch (Exception e) {
            // since we don't have a logger available, lets log it to the System.err
            e.printStackTrace();
            return null;
        }
    }
}
