/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

public final class Logger {

    private static volatile LoggerFactory loggerFactory;

    private static final Object FACTORY_LOCK = new Object();

    private Logger() {
    }

    /**
     * @param clazz class to take the logger name from
     * @return the logger
     * Use LoggingService instead if possible. Do not log in static context if possible.
     */
    public static ILogger getLogger(Class clazz) {
        return getLogger(clazz.getName());
    }

    /**
     * @param name name of the logger
     * @return the logger
     * Use LoggingService instead if possible. Do not log in static context if possible.
     */
    public static ILogger getLogger(String name) {
        LoggerFactory loggerFactoryToUse = loggerFactory;
        // fast-track when factory is initialized
        if (loggerFactoryToUse != null) {
            return loggerFactoryToUse.getLogger(name);
        }

        synchronized (FACTORY_LOCK) {
            if (loggerFactory == null) {
                // init static logger with user-specified custom logger class
                String loggerClass = System.getProperty("hazelcast.logging.class");
                if (!StringUtil.isNullOrEmpty(loggerClass)) {
                    // ok, there is a custom logging factory class -> let's use it now and store it for next time
                    loggerFactoryToUse = loadLoggerFactory(loggerClass);
                    loggerFactory = loggerFactoryToUse;
                } else {
                    String loggerType = System.getProperty("hazelcast.logging.type");
                    loggerFactoryToUse = newLoggerFactory(loggerType);
                    // store the factory only when the type was set explicitly. we do not want to store default type.
                    if (!StringUtil.isNullOrEmpty(loggerType)) {
                        loggerFactory = loggerFactoryToUse;
                    }
                }
            }
        }
        // loggerFactory was initialized by other thread before we acquired the lock -> loggerFactoryToUse was left to null
        if (loggerFactoryToUse == null) {
            loggerFactoryToUse = loggerFactory;
        }
        return loggerFactoryToUse.getLogger(name);
    }

    public static ILogger noLogger() {
        return new NoLogFactory.NoLogger();
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    public static LoggerFactory newLoggerFactory(String loggerType) {
        LoggerFactory loggerFactory = null;
        String loggerClass = System.getProperty("hazelcast.logging.class");
        if (loggerClass != null) {
            loggerFactory = loadLoggerFactory(loggerClass);
        }

        if (loggerFactory == null) {
            if (loggerType != null) {
                if ("log4j".equals(loggerType)) {
                    loggerFactory = loadLoggerFactory("com.hazelcast.logging.Log4jFactory");
                } else if ("log4j2".equals(loggerType)) {
                    loggerFactory = loadLoggerFactory("com.hazelcast.logging.Log4j2Factory");
                } else if ("slf4j".equals(loggerType)) {
                    loggerFactory = loadLoggerFactory("com.hazelcast.logging.Slf4jFactory");
                } else if ("jdk".equals(loggerType)) {
                    loggerFactory = new StandardLoggerFactory();
                } else if ("none".equals(loggerType)) {
                    loggerFactory = new NoLogFactory();
                }
            }
        }

        if (loggerFactory == null) {
            loggerFactory = new StandardLoggerFactory();
        }

        // init static field as early as possible
        if (Logger.loggerFactory == null) {
            //noinspection SynchronizationOnStaticField
            synchronized (FACTORY_LOCK) {
                if (Logger.loggerFactory == null) {
                    Logger.loggerFactory = loggerFactory;
                }
            }
        }

        return loggerFactory;
    }

    private static LoggerFactory loadLoggerFactory(String className) {
        try {
            return ClassLoaderUtil.newInstance(null, className);
        } catch (Exception e) {
            // since we don't have a logger available, lets log it to the System.err
            e.printStackTrace();
            return null;
        }
    }
}
