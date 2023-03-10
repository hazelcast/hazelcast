/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.test.IsolatedLoggingRule;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static com.hazelcast.internal.TestSupport.assertInstanceOf;
import static com.hazelcast.test.ClassTestSupport.assertUtilityConstructor;

/**
 * Unit tests for {@link com.hazelcast.logging.Logger} class.
 */

//@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class LoggerTest {

    @Rule
    public final IsolatedLoggingRule isolatedLoggingRule = new IsolatedLoggingRule();

    @Test
    public void testConstructor() {
        assertUtilityConstructor(Logger.class);
    }

    @Test
    public void getLogger_thenLog4j_thenReturnLog4jLogger() {
        isolatedLoggingRule.setLoggingType(IsolatedLoggingRule.LOGGING_TYPE_LOG4J);
        assertInstanceOf(Log4jFactory.Log4jLogger.class, Logger.getLogger(getClass()));
    }

    @Test
    public void getLogger_thenLog4j2_thenReturnLog4j2Logger() {
        isolatedLoggingRule.setLoggingType(IsolatedLoggingRule.LOGGING_TYPE_LOG4J2);
        assertInstanceOf(Log4j2Factory.Log4j2Logger.class, Logger.getLogger(getClass()));
    }

    @Test
    public void getLogger_whenSlf4j_thenReturnSlf4jLogger() {
        isolatedLoggingRule.setLoggingType(IsolatedLoggingRule.LOGGING_TYPE_SLF4J);
        assertInstanceOf(Slf4jFactory.Slf4jLogger.class, Logger.getLogger(getClass()));
    }

    @Test
    public void getLogger_whenJdk_thenReturnStandardLogger() {
        isolatedLoggingRule.setLoggingType(IsolatedLoggingRule.LOGGING_TYPE_JDK);
        assertInstanceOf(StandardLoggerFactory.StandardLogger.class, Logger.getLogger(getClass()));
    }

    @Test
    public void getLogger_whenNone_thenReturnNoLogger() {
        isolatedLoggingRule.setLoggingType(IsolatedLoggingRule.LOGGING_TYPE_NONE);
        assertInstanceOf(NoLogFactory.NoLogger.class, Logger.getLogger(getClass()));
    }

    @Test
    public void getLogger_whenInvalidConfiguration_thenCreateStandardLogger() {
        isolatedLoggingRule.setLoggingType("invalid");
        assertInstanceOf(StandardLoggerFactory.StandardLogger.class, Logger.getLogger(getClass()));
    }

    @Test
    public void noLogger() {
        assertInstanceOf(NoLogFactory.NoLogger.class, Logger.noLogger());
    }


    @Test
    public void newLoggerFactory_whenClassConfigured_thenShareLoggerFactoryWithGetLogger() {
        isolatedLoggingRule.setLoggingClass(Log4j2Factory.class);

        final ILogger loggerViaGetLogger = Logger.getLogger(getClass().getName());
        final LoggerFactory loggerFactory = Logger.newLoggerFactory("irrelevant");
        final ILogger loggerViaFactory = loggerFactory.getLogger(getClass().getName());

        assertInstanceOf(Log4j2Factory.Log4j2Logger.class, loggerViaGetLogger);
        assertInstanceOf(Log4j2Factory.Log4j2Logger.class, loggerViaFactory);
        Assert.assertEquals(loggerFactory, isolatedLoggingRule.getLoggerFactory());
    }

    @Test
    public void newLoggerFactory_whenTypeConfigured_thenShareLoggerFactoryWithGetLoggerIfTypesMatch() {
        isolatedLoggingRule.setLoggingType(IsolatedLoggingRule.LOGGING_TYPE_LOG4J2);

        final ILogger loggerViaGetLogger = Logger.getLogger(getClass().getName());
        final LoggerFactory loggerFactory = Logger.newLoggerFactory(IsolatedLoggingRule.LOGGING_TYPE_LOG4J2);
        final ILogger loggerViaFactory = loggerFactory.getLogger(getClass().getName());

        assertInstanceOf(Log4j2Factory.Log4j2Logger.class, loggerViaGetLogger);
        assertInstanceOf(Log4j2Factory.Log4j2Logger.class, loggerViaFactory);
        Assert.assertEquals(loggerFactory, isolatedLoggingRule.getLoggerFactory());
    }

    @Test
    public void newLoggerFactory_whenTypeConfigured_thenDoNotShareLoggerFactoryWithGetLoggerIfTypesDoNotMatch() {
        isolatedLoggingRule.setLoggingType(IsolatedLoggingRule.LOGGING_TYPE_LOG4J2);

        final ILogger loggerViaGetLogger = Logger.getLogger(getClass().getName());
        final LoggerFactory loggerFactory = Logger.newLoggerFactory(IsolatedLoggingRule.LOGGING_TYPE_LOG4J);
        final ILogger loggerViaFactory = loggerFactory.getLogger(getClass().getName());

        assertInstanceOf(Log4j2Factory.Log4j2Logger.class, loggerViaGetLogger);
        assertInstanceOf(Log4jFactory.Log4jLogger.class, loggerViaFactory);
        Assert.assertNotEquals(loggerFactory, isolatedLoggingRule.getLoggerFactory());
    }
}
