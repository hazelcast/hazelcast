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

    /**
     * The goal of this field is to share the common global factory instance to allow access to it from the static context.
     * <ul>
     * <li>If the class of the factory is configured using {@code hazelcast.logging.class} in {@link System#getProperties}, all
     * {@link com.hazelcast.core.HazelcastInstance HazelcastInstance}s will share this single instance.
     * <li>If the type of the factory is configured using {@code hazelcast.logging.type} in {@link System#getProperties}, all
     * {@link com.hazelcast.core.HazelcastInstance HazelcastInstance}s that have the same logging type configured will share
     * this single instance.
     * <li>If neither the logging class nor the logging type is configured in {@link System#getProperties}, this field will
     * store a factory constructed during the first invocation of {@link #newLoggerFactory}, which is typically done during the
     * construction of the first {@link com.hazelcast.core.HazelcastInstance HazelcastInstance}. In this case, this factory
     * instance will be shared with the constructed {@link com.hazelcast.core.HazelcastInstance HazelcastInstance}, all other
     * {@link com.hazelcast.core.HazelcastInstance HazelcastInstance}s will receive their own logging factories.
     * </ul>
     */
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
        // try the fast path first
        LoggerFactory existingFactory = loggerFactory;
        if (existingFactory != null) {
            return existingFactory.getLogger(name);
        }

        synchronized (FACTORY_LOCK) {
            existingFactory = loggerFactory;
            if (existingFactory != null) {
                return existingFactory.getLogger(name);
            }

            LoggerFactory createdFactory = null;

            final String loggingClass = System.getProperty("hazelcast.logging.class");
            if (!StringUtil.isNullOrEmpty(loggingClass)) {
                createdFactory = tryCreateLoggerFactory(loggingClass);
            }

            if (createdFactory != null) {
                // hazelcast.logging.class property has the highest priority, so it's safe to store the factory for reuse
                loggerFactory = createdFactory;
            } else {
                final String loggingType = System.getProperty("hazelcast.logging.type");
                createdFactory = createLoggerFactory(loggingType);

                if (!StringUtil.isNullOrEmpty(loggingType)) {
                    // hazelcast.logging.type property is the 2nd by priority, so it's safe to store the factory for reuse
                    loggerFactory = createdFactory;
                }
            }

            return createdFactory.getLogger(name);
        }
    }

    /**
     * @return the no operation logger, which ignores all logging requests.
     */
    public static ILogger noLogger() {
        return new NoLogFactory.NoLogger();
    }

    /**
     * Creates a {@link LoggerFactory logger factory} instance of the given preferred type.
     * <p>
     * The type of the created logger factory doesn't necessarily match the given preferred type. For example, if
     * {@code hazelcast.logging.class} system property is set, the configured logger factory class will be used
     * despite the given preferred type. Also, if factory instance construction is failed for some reason,
     * {@link StandardLoggerFactory} instance will be returned instead.
     *
     * @param preferredType the preferred type of the logger factory to create.
     * @return the created logger factory.
     */
    public static LoggerFactory newLoggerFactory(String preferredType) {
        // creation of new logger factory is not a frequent operation, so we can afford doing this under the lock
        synchronized (FACTORY_LOCK) {
            final LoggerFactory existingFactory = loggerFactory;
            LoggerFactory createdFactory = null;

            final String loggingClass = System.getProperty("hazelcast.logging.class");
            if (!StringUtil.isNullOrEmpty(loggingClass)) {
                if (existingFactory != null) {
                    // hazelcast.logging.class property has the highest priority, so it's safe to return the shared factory
                    return existingFactory;
                }

                createdFactory = tryCreateLoggerFactory(loggingClass);
            }

            if (createdFactory == null) {
                final String loggingType = System.getProperty("hazelcast.logging.type");
                //noinspection StringEquality
                if (existingFactory != null && !StringUtil.isNullOrEmpty(loggingType) && loggingType.equals(preferredType)) {
                    // hazelcast.logging.type property is the 2nd by priority, so it's safe to return the shared factory
                    return existingFactory;
                }

                createdFactory = createLoggerFactory(preferredType);
            }

            // initialize the global shared logger factory as early as possible to maximize the chances of sharing it in the
            // static context, if only a single Hazelcast instance is created, which is the most common case
            if (existingFactory == null) {
                loggerFactory = createdFactory;
            }

            return createdFactory;
        }
    }

    private static LoggerFactory createLoggerFactory(String preferredType) {
        final LoggerFactory createdFactory;

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
        } else {
            createdFactory = new StandardLoggerFactory();
        }

        return createdFactory;
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
