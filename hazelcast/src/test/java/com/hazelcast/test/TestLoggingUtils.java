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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractOutputStreamAppender;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.appender.OutputStreamManager;

import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.Map;

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

            hackLog4j();
        }
    }

    private static void hackLog4j()  {
        LoggerContext context = (LoggerContext) LogManager.getContext();
        Map<String, Appender> allAppenders = context.getConfiguration().getAppenders();
        Appender appender = allAppenders.get("Console");
        if (appender == null) {
            System.err.println(TestLoggingUtils.class.getName() + ": Console appender not found. ");
        } else if (appender instanceof ConsoleAppender) {
            ConsoleAppender consoleAppender = (ConsoleAppender) appender;
            if (consoleAppender instanceof ConsoleAppender) {
                try {
                    Field flushField = AbstractOutputStreamAppender.class.getDeclaredField("immediateFlush");
                    if (!flushField.isAccessible()) {
                        flushField.setAccessible(true);
                    }
                    flushField.set(consoleAppender, false);

                    Field managerField = AbstractOutputStreamAppender.class.getDeclaredField("manager");
                    if (!managerField.isAccessible()) {
                        managerField.setAccessible(true);
                    }
                    OutputStreamManager manager = (OutputStreamManager) managerField.get(consoleAppender);
                    Field osField = OutputStreamManager.class.getDeclaredField("os");
                    if (!osField.isAccessible()) {
                        osField.setAccessible(true);
                    }
                    OutputStream originalOS = (OutputStream) osField.get(manager);
                    BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(originalOS, 64 * 1024);
                    osField.set(manager, bufferedOutputStream);
                } catch (NoSuchFieldException e) {
                    throw new AssertionError("Did you just change log4j2 version? If so then this hack has to be adjusted");
                } catch (IllegalAccessException e) {
                    throw new AssertionError("Did you just change log4j2 version? If so then this hack has to be adjusted");
                }
            }
        } else {
            System.err.println(TestLoggingUtils.class.getName() + ": Console appender is type of " + appender.getClass().getName());
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
