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

import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.util.StringUtil;

import javax.annotation.Nonnull;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Provides static utilities to access the global shared logging machinery.
 * <p>
 * Note: if possible, avoid logging in the global shared context exposed by the utilities of this class,
 * use {@link LoggingService} instead.
 */
public final class Logger {

    /**
     * The goal of this field is to share the common global factory instance to allow access to it from the static context and
     * from the {@link LoggingService} context.
     * <ul>
     * <li>If the class of the factory is configured using {@code hazelcast.logging.class} in {@link System#getProperties}, all
     * {@link com.hazelcast.core.HazelcastInstance HazelcastInstance}s will share this single instance.
     * <li>If the type of the factory is configured using {@code hazelcast.logging.type} in {@link System#getProperties} and
     * {@link #getLogger(String) getLogger} is invoked before the first call to {@link #newLoggerFactory}, all
     * {@link com.hazelcast.core.HazelcastInstance HazelcastInstance}s that have the same logging type configured will share
     * this single instance.
     * <li>If the first invocation of {@link #newLoggerFactory} is done before a call to {@link #getLogger(String) getLogger},
     * this field will store a factory constructed during this first invocation of {@link #newLoggerFactory}, which is typically
     * done during the construction of the first {@link com.hazelcast.core.HazelcastInstance HazelcastInstance}. In this case,
     * this factory instance will be shared with all {@link com.hazelcast.core.HazelcastInstance HazelcastInstance}s having the
     * same logging type configured, all other {@link com.hazelcast.core.HazelcastInstance HazelcastInstance}s will receive their
     * own logging factories.
     * </ul>
     */
    private static volatile LoggerFactory loggerFactory;

    private static String loggerFactoryClassOrType;

    private static final Object FACTORY_LOCK = new Object();

    private Logger() {
    }

    /**
     * Obtains a {@link ILogger logger} for the given {@code clazz}.
     *
     * @param clazz the class to obtain the logger for.
     * @return the obtained logger.
     */
    public static ILogger getLogger(@Nonnull Class clazz) {
        checkNotNull(clazz, "class must not be null");
        return getLoggerInternal(clazz.getName());
    }

    /**
     * Obtains a {@link ILogger logger} of the given {@code name}.
     *
     * @param name the name of the logger to obtain.
     * @return the obtained logger.
     */
    public static ILogger getLogger(@Nonnull String name) {
        checkNotNull(name, "name must not be null");
        return getLoggerInternal(name);
    }

    private static ILogger getLoggerInternal(String name) {
        LoggerFactory existingFactory = loggerFactory;
        if (existingFactory != null) {
            return existingFactory.getLogger(name);
        }
        return createFactoryInternal().getLogger(name);
    }

    private static LoggerFactory createFactoryInternal() {
        synchronized (FACTORY_LOCK) {
            LoggerFactory existingFactory = loggerFactory;
            if (existingFactory != null) {
                return existingFactory;
            }

            LoggerFactory createdFactory = null;

            final String loggingClass = System.getProperty("hazelcast.logging.class");
            if (!StringUtil.isNullOrEmpty(loggingClass)) {
                createdFactory = tryToCreateLoggerFactory(loggingClass);
            }

            if (createdFactory != null) {
                // hazelcast.logging.class property overrides everything, so it's safe to store the factory for reuse
                loggerFactory = createdFactory;
                loggerFactoryClassOrType = loggingClass;
            } else {
                final String loggingType = System.getProperty("hazelcast.logging.type");
                createdFactory = createLoggerFactory(loggingType);

                if (!StringUtil.isNullOrEmpty(loggingType)) {
                    // hazelcast.logging.type property is the recommended way of configuring the logging, in most setups
                    // the configured value should match with the value passed to newLoggerFactory(), so we store the created
                    // factory for reuse
                    loggerFactory = createdFactory;
                    loggerFactoryClassOrType = loggingType;
                }
            }
            return createdFactory;
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
        // creation of a new logger factory is not a performance critical operation, so we can afford doing it under the lock
        // and perform many non-cached accesses to volatile loggerFactory field to simplify the code

        synchronized (FACTORY_LOCK) {
            LoggerFactory obtainedFactory;

            obtainedFactory = tryToObtainFactoryByConfiguredClass();
            if (obtainedFactory != null) {
                return obtainedFactory;
            }

            obtainedFactory = tryToObtainFactoryByPreferredType(preferredType);
            if (obtainedFactory != null) {
                return obtainedFactory;
            }

            assert StringUtil.isNullOrEmpty(preferredType);
            obtainedFactory = obtainFactoryByRecoveringFromNullOrEmptyPreferredType();

            return obtainedFactory;
        }
    }

    private static LoggerFactory tryToObtainFactoryByConfiguredClass() {
        final String loggingClass = System.getProperty("hazelcast.logging.class");

        if (!StringUtil.isNullOrEmpty(loggingClass)) {
            if (sharedFactoryIsCompatibleWith(loggingClass)) {
                return loggerFactory;
            }

            final LoggerFactory createdFactory = tryToCreateLoggerFactory(loggingClass);
            if (createdFactory != null) {
                if (loggerFactory == null) {
                    loggerFactory = createdFactory;
                    loggerFactoryClassOrType = loggingClass;
                }
                return createdFactory;
            }
        }

        return null;
    }

    private static LoggerFactory tryToObtainFactoryByPreferredType(String preferredType) {
        if (!StringUtil.isNullOrEmpty(preferredType)) {
            if (sharedFactoryIsCompatibleWith(preferredType)) {
                return loggerFactory;
            }

            final LoggerFactory createdFactory = createLoggerFactory(preferredType);
            if (loggerFactory == null) {
                loggerFactory = createdFactory;
                loggerFactoryClassOrType = preferredType;
            }
            return createdFactory;
        }

        return null;
    }

    private static LoggerFactory obtainFactoryByRecoveringFromNullOrEmptyPreferredType() {
        if (loggerFactory != null) {
            return loggerFactory;
        }

        final String loggingType = System.getProperty("hazelcast.logging.type");
        if (!StringUtil.isNullOrEmpty(loggingType)) {
            final LoggerFactory createdFactory = createLoggerFactory(loggingType);
            loggerFactory = createdFactory;
            loggerFactoryClassOrType = loggingType;
            return createdFactory;
        }

        final LoggerFactory createdFactory = new StandardLoggerFactory();
        loggerFactory = createdFactory;
        loggerFactoryClassOrType = "jdk";
        return createdFactory;
    }

    private static boolean sharedFactoryIsCompatibleWith(String requiredClassOrType) {
        return loggerFactory != null && !StringUtil.isNullOrEmpty(loggerFactoryClassOrType) && loggerFactoryClassOrType
                .equals(requiredClassOrType);
    }

    private static LoggerFactory createLoggerFactory(String preferredType) {
        LoggerFactory createdFactory;

        if ("log4j".equals(preferredType)) {
            createdFactory = tryToCreateLoggerFactory("com.hazelcast.logging.Log4jFactory");
        } else if ("log4j2".equals(preferredType)) {
            createdFactory = tryToCreateLoggerFactory("com.hazelcast.logging.Log4j2Factory");
        } else if ("slf4j".equals(preferredType)) {
            createdFactory = tryToCreateLoggerFactory("com.hazelcast.logging.Slf4jFactory");
        } else if ("jdk".equals(preferredType)) {
            createdFactory = new StandardLoggerFactory();
        } else if ("none".equals(preferredType)) {
            createdFactory = new NoLogFactory();
        } else {
            if (!StringUtil.isNullOrEmpty(preferredType)) {
                logError("Unexpected logging type '" + preferredType + "', falling back to JDK logging.", null);
            }
            createdFactory = new StandardLoggerFactory();
        }

        if (createdFactory == null) {
            logError("Falling back to JDK logging.", null);
            createdFactory = new StandardLoggerFactory();
        }

        return createdFactory;
    }

    private static LoggerFactory tryToCreateLoggerFactory(String className) {
        try {
            return ClassLoaderUtil.newInstance(null, className);
        } catch (Exception e) {
            logError("Failed to create '" + className + "' logger factory:", e);
            return null;
        }
    }

    private static void logError(String message, Throwable cause) {
        // We may try to use the existing shared logger factory here, but in
        // this case the error may end up in a pretty unexpected place. Just log
        // to stderr.

        System.err.println(message);
        if (cause != null) {
            cause.printStackTrace();
        }
    }

}
