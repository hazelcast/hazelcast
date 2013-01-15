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

public class Logger {
    static private volatile LoggerFactory loggerFactory = null;
    static private final Object factoryLock = new Object();

    public static ILogger getLogger(String name) {
        //noinspection DoubleCheckedLocking
        if (loggerFactory == null) {
            //noinspection SynchronizationOnStaticField
            synchronized (factoryLock) {
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
            try {
                loggerFactory = (LoggerFactory) ClassLoaderUtil.loadClass(loggerClass).newInstance();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (loggerFactory == null) {
            if (loggerType != null) {
                if ("log4j".equals(loggerType)) {
                    try {
                        loggerFactory = (LoggerFactory) ClassLoaderUtil.loadClass("com.hazelcast.logging.Log4jFactory").newInstance();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else if ("slf4j".equals(loggerType)) {
                    try {
                        loggerFactory = (LoggerFactory) ClassLoaderUtil.loadClass("com.hazelcast.logging.Slf4jFactory").newInstance();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
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
}
