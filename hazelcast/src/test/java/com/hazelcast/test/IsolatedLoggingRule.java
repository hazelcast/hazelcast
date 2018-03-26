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

package com.hazelcast.test;

import com.hazelcast.logging.Logger;
import com.hazelcast.logging.LoggerFactory;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.lang.reflect.Field;

/**
 * Fully isolates the shared global logging state for a test. Also provides
 * utilities which are useful while testing logging related things.
 * <p>
 * Make sure you are using a serial test runner if you use this rule.
 */
public class IsolatedLoggingRule implements TestRule {

    public static final String LOGGING_TYPE_PROPERTY = "hazelcast.logging.type";
    public static final String LOGGING_CLASS_PROPERTY = "hazelcast.logging.class";

    public static final String LOGGING_TYPE_LOG4J = "log4j";
    public static final String LOGGING_TYPE_LOG4J2 = "log4j2";
    public static final String LOGGING_TYPE_SLF4J = "slf4j";
    public static final String LOGGING_TYPE_JDK = "jdk";
    public static final String LOGGING_TYPE_NONE = "none";

    private final Field loggerFactoryField;

    public IsolatedLoggingRule() {
        loggerFactoryField = getLoggerField("loggerFactory");
    }

    @Override
    public Statement apply(final Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                String oldLoggingType = System.getProperty(LOGGING_TYPE_PROPERTY);
                String oldLoggingClass = System.getProperty(LOGGING_CLASS_PROPERTY);

                System.clearProperty(LOGGING_TYPE_PROPERTY);
                System.clearProperty(LOGGING_CLASS_PROPERTY);

                LoggerFactory originalLoggerFactory = (LoggerFactory) loggerFactoryField.get(null);

                loggerFactoryField.set(null, null);

                try {
                    base.evaluate();
                } finally {
                    setOrClearProperty(LOGGING_TYPE_PROPERTY, oldLoggingType);
                    setOrClearProperty(LOGGING_CLASS_PROPERTY, oldLoggingClass);

                    loggerFactoryField.set(null, originalLoggerFactory);
                }
            }
        };
    }

    public void setLoggingType(String type) {
        setOrClearProperty(LOGGING_TYPE_PROPERTY, type);
    }

    public void setLoggingClass(Class<? extends LoggerFactory> class_) {
        setOrClearProperty(LOGGING_CLASS_PROPERTY, class_ == null ? null : class_.getName());
    }

    private void setOrClearProperty(String propertyName, String value) {
        if (value == null) {
            System.clearProperty(propertyName);
        } else {
            System.setProperty(propertyName, value);
        }
    }

    private Field getLoggerField(String name) {
        Field field;
        try {
            field = Logger.class.getDeclaredField(name);
        } catch (NoSuchFieldException e) {
            throw new IllegalStateException(name + " field not found");
        }
        field.setAccessible(true);
        return field;
    }

}
