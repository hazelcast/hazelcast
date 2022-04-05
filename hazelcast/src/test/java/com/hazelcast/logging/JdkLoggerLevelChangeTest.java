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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static com.hazelcast.test.IsolatedLoggingRule.LOGGING_TYPE_JDK;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class JdkLoggerLevelChangeTest extends HazelcastTestSupport {

    @Rule
    public final IsolatedLoggingRule isolatedLoggingRule = new IsolatedLoggingRule();

    private LoggingServiceImpl loggingService;
    private TestHandler handler;
    private ILogger logger;

    @Before
    public void before() {
        isolatedLoggingRule.setLoggingType(LOGGING_TYPE_JDK);
        HazelcastInstance instance = createHazelcastInstance();
        loggingService = (LoggingServiceImpl) instance.getLoggingService();

        handler = new TestHandler();
        Logger.getLogger(JdkLoggerLevelChangeTest.class.getName()).addHandler(handler);

        logger = loggingService.getLogger(JdkLoggerLevelChangeTest.class.getName());
    }

    @Test
    public void test() {
        assertEquals(0, handler.hits);

        logger.finest("foo");
        assertEquals(0, handler.hits);

        logger.severe("foo");
        assertEquals(1, handler.hits);

        loggingService.setLevel(Level.OFF);
        logger.severe("foo");
        assertEquals(2, handler.hits);

        loggingService.setLevel(Level.FINEST);
        logger.finest("foo");
        assertEquals(3, handler.hits);

        loggingService.resetLevel();
        logger.finest("foo");
        assertEquals(3, handler.hits);

        logger.severe("foo");
        assertEquals(4, handler.hits);
    }

    private static class TestHandler extends Handler {

        public int hits;

        @Override
        public void publish(LogRecord record) {
            if (JdkLoggerLevelChangeTest.class.getName().equals(record.getLoggerName())) {
                hits += 1;
            }
        }

        @Override
        public void flush() {

        }

        @Override
        public void close() throws SecurityException {

        }

    }

}
