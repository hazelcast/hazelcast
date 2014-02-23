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

import com.hazelcast.nio.ClassLoaderUtil;

public final class Logger {

    private static volatile LoggerFactory loggerFactory;
    private static final Object FACTORY_LOCK = new Object();

    private Logger() {
    }

    public static ILogger getLogger(Class clazz) {
        return getLogger(clazz.getName());
    }

    public static ILogger getLogger(String name) {
        //noinspection DoubleCheckedLocking
        if (loggerFactory == null) {
            //noinspection SynchronizationOnStaticField
            synchronized (FACTORY_LOCK) {
                if (loggerFactory == null) {
                    String loggerType = System.getProperty("hazelcast.logging.type");
                    loggerFactory = newLoggerFactory(loggerType);
                }
            }
        }
        return loggerFactory.getLogger(name);
    }

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
        return loggerFactory;
    }

    private static LoggerFactory loadLoggerFactory(String className) {
        try {
            return ClassLoaderUtil.newInstance(null, className);
        } catch (Exception e) {
            //since we don't have a logger available, lets log it to the System.err
            e.printStackTrace();
            return null;
        }
    }
}
