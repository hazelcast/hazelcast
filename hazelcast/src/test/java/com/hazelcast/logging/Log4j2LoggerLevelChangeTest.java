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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.impl.LoggingServiceImpl;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.IsolatedLoggingRule;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.logging.Level;

import static com.hazelcast.test.IsolatedLoggingRule.LOGGING_TYPE_LOG4J2;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class Log4j2LoggerLevelChangeTest extends HazelcastTestSupport {

    @Rule
    public final IsolatedLoggingRule isolatedLoggingRule = new IsolatedLoggingRule();

    private LoggingServiceImpl loggingService;
    private TestAppender appender;
    private ILogger logger;

    @Before
    public void before() {
        isolatedLoggingRule.setLoggingType(LOGGING_TYPE_LOG4J2);
        HazelcastInstance instance = createHazelcastInstance();
        loggingService = (LoggingServiceImpl) instance.getLoggingService();

        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        Configuration configuration = context.getConfiguration();
        appender = new TestAppender();
        appender.start();
        configuration.addAppender(appender);
        configuration.getRootLogger().addAppender(appender, null, null);

        logger = loggingService.getLogger(Log4j2LoggerLevelChangeTest.class.getName());
    }

    @Test
    public void test() {
        assertEquals(0, appender.hits);

        logger.finest("foo");
        assertEquals(0, appender.hits);

        logger.severe("foo");
        assertEquals(1, appender.hits);

        loggingService.setLevel(Level.OFF);
        logger.severe("foo");
        assertEquals(2, appender.hits);

        loggingService.setLevel(Level.FINEST);
        logger.finest("foo");
        assertEquals(3, appender.hits);

        loggingService.resetLevel();
        logger.finest("foo");
        assertEquals(3, appender.hits);

        logger.severe("foo");
        assertEquals(4, appender.hits);
    }

    private static class TestAppender extends AbstractAppender {

        public int hits;

        TestAppender() {
            super(TestAppender.class.getName(), null, null, true, null);
        }

        @Override
        public void append(LogEvent event) {
            if (Log4j2LoggerLevelChangeTest.class.getName().equals(event.getLoggerName())) {
                hits += 1;
            }
        }

    }

}
