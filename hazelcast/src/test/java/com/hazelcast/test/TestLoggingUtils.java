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

package com.hazelcast.test;

import org.apache.logging.log4j.ThreadContext;

public final class TestLoggingUtils {
    private static String LOGGING_TYPE_PROP_NAME = "hazelcast.logging.type";
    private static String LOGGING_CLASS_PROP_NAME = "hazelcast.logging.class";

    private static final boolean IS_LOG4J2_AVAILABLE = isClassAvailable("org.apache.logging.log4j.Logger");
    private static final boolean IS_DISRUPTOR_AVAILABLE = isClassAvailable("com.lmax.disruptor.dsl.Disruptor");

    private TestLoggingUtils() {
    }

    public static void initializeLogging() {
        if (shouldForceTestLoggingFactory()) {
            configureAsyncLogging();
            String factoryClassName = TestLoggerFactory.class.getName();
            System.setProperty(LOGGING_CLASS_PROP_NAME, factoryClassName);
            System.setProperty("isThreadContextMapInheritable", "true");
            System.clearProperty(LOGGING_TYPE_PROP_NAME);
        }
    }

    private static void configureAsyncLogging() {
        if (IS_DISRUPTOR_AVAILABLE) {
            System.setProperty("Log4jContextSelector", "org.apache.logging.log4j.core.async.AsyncLoggerContextSelector");
        }
    }

    static void setThreadLocalTestMethodName(String methodName) {
        if (IS_LOG4J2_AVAILABLE) {
            ThreadContext.put("test-name", methodName);
        }
    }

    static void removeThreadLocalTestMethodName() {
        if (IS_LOG4J2_AVAILABLE) {
            ThreadContext.remove("test-name");
        }
    }

    private static boolean isClassAvailable(String classname) {
        try {
            Class.forName(classname);
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    private static boolean shouldForceTestLoggingFactory() {
        if (!IS_LOG4J2_AVAILABLE) {
            return false;
        }
        if (JenkinsDetector.isOnJenkins()) {
            return true;
        }
        if (System.getProperty(LOGGING_TYPE_PROP_NAME) == null && System.getProperty(LOGGING_CLASS_PROP_NAME) == null) {
            return true;
        }
        return false;
    }


}
