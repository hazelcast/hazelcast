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
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.appender.OutputStreamManager;

import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.util.Map;

import static com.hazelcast.test.HazelcastTestSupport.getFromField;
import static com.hazelcast.test.HazelcastTestSupport.setToField;

public final class TestLoggingUtils {
    private static String LOGGING_TYPE_PROP_NAME = "hazelcast.logging.type";
    private static String LOGGING_CLASS_PROP_NAME = "hazelcast.logging.class";

    private static final boolean IS_LOG4J2_AVAILABLE = isClassAvailable("org.apache.logging.log4j.Logger");

    private TestLoggingUtils() {
    }

    public static void initializeLogging() {
        if (shouldForceTestLoggingFactory()) {
            String factoryClassName = TestLoggerFactory.class.getName();
            System.setProperty(LOGGING_CLASS_PROP_NAME, factoryClassName);
            System.setProperty("isThreadContextMapInheritable", "true");
            System.clearProperty(LOGGING_TYPE_PROP_NAME);

            hackLog4jConsoleAppender();
        }
    }

    private static void hackLog4jConsoleAppender()  {
        // Maven Surefire plugin redirects Console output to a file
        // unfortunately it has no buffering - logging produces
        // high amount of I/O operaitons, this eats AWS I/O burst credits
        // and logging becomes dog-slow. A single logging statement can take
        // 2-10 seconds.

        // this hack attempts to make this issue less severe:
        // 1. hack the Console appender to disable calling flush() after each logging statement
        // 2. wrap the OutputStream inside BufferedOutputStream

        // this hack may break in future log4j versions!

        // Alternative would be using e.g. RollingAppender instead of the Console appender
        // The RollingAppender has buffering built-in. However Console appender has one massive
        // advantage: the Surefire Maven plugin redirects Console and produces a file
        // per test class -> this helps a lot in test log analysis.
        LoggerContext context = (LoggerContext) LogManager.getContext();
        Map<String, Appender> allAppenders = context.getConfiguration().getAppenders();
        Appender appender = allAppenders.get("Console");
        if (appender == null) {
            System.err.println(TestLoggingUtils.class.getName() + ": Console appender not found. ");
        } else if (!(appender instanceof ConsoleAppender)) {
            System.err.println(TestLoggingUtils.class.getName() + ": Console appender is type of " + appender.getClass().getName());
        } else {
            ConsoleAppender consoleAppender = (ConsoleAppender) appender;
            setToField(consoleAppender, "immediateFlush", true);
            OutputStreamManager manager = getFromField(consoleAppender, "manager");
            OutputStream originalOS = getFromField(manager, "os");
            BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(originalOS, 64 * 1024);
            setToField(manager, "os", bufferedOutputStream);
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
