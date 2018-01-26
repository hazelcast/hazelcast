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

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Field;

import static com.hazelcast.spi.properties.GroupProperty.LOGGING_TYPE;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class LoggerStressTest extends HazelcastTestSupport {

    private static final int TEST_DURATION_SECONDS = 5;
    private static final int THREAD_COUNT = 4;

    private static final String LOGGING_CLASS_PROP_NAME = "hazelcast.logging.class";

    private String originalLoggingClass;
    private String originalLoggingType;
    private LoggerFactory originLoggerFactory;

    @Before
    public void setUp() throws Exception {
        originalLoggingClass = System.getProperty(LOGGING_CLASS_PROP_NAME);
        originalLoggingType = System.getProperty(LOGGING_TYPE.getName());
        originLoggerFactory = (LoggerFactory) getLoggerFactoryField().get(null);
    }

    @After
    public void tearDown() throws Exception {
        restoreProperty(LOGGING_CLASS_PROP_NAME, originalLoggingClass);
        restoreProperty(LOGGING_TYPE.getName(), originalLoggingType);
        getLoggerFactoryField().set(null, originLoggerFactory);
    }

    @Test
    public void stressThreadsWithCustomLoggingClass() throws Exception {
        System.setProperty(LOGGING_CLASS_PROP_NAME, LoggingFactoryStub.class.getName());

        long deadLine = System.currentTimeMillis() + SECONDS.toMillis(TEST_DURATION_SECONDS);
        StressThread[] threads = startStressThreads(deadLine);
        assertThreadsEventuallyFinishesWithoutException(threads);
    }

    @Test
    public void stressThreadsWithLoggingType() throws Exception {
        System.setProperty(LOGGING_TYPE.getName(), "log4j");

        long deadLine = System.currentTimeMillis() + SECONDS.toMillis(TEST_DURATION_SECONDS);
        StressThread[] threads = startStressThreads(deadLine);
        assertThreadsEventuallyFinishesWithoutException(threads);
    }

    private static Field getLoggerFactoryField() {
        Field loggerFactoryField;
        try {
            loggerFactoryField = Logger.class.getDeclaredField("loggerFactory");
        } catch (NoSuchFieldException e) {
            throw new AssertionError("LoggerFactory field not found");
        }
        loggerFactoryField.setAccessible(true);
        return loggerFactoryField;
    }

    private static void restoreProperty(String name, String value) {
        if (value == null) {
            System.clearProperty(name);
        } else {
            System.setProperty(name, value);
        }
    }

    private static StressThread[] startStressThreads(long deadLine) {
        StressThread[] threads = new StressThread[THREAD_COUNT];
        for (int i = 0; i < THREAD_COUNT; i++) {
            threads[i] = new StressThread(deadLine);
        }
        for (int i = 0; i < THREAD_COUNT; i++) {
            threads[i].start();
        }
        return threads;
    }

    private static void assertThreadsEventuallyFinishesWithoutException(StressThread[] threads) throws Exception {
        for (StressThread thread : threads) {
            thread.join();
            Exception exception = thread.e;
            if (exception != null) {
                throw exception;
            }
        }
    }

    private static class LoggingFactoryStub implements LoggerFactory {
        @Override
        public ILogger getLogger(String name) {
            return mock(ILogger.class, withSettings().stubOnly());
        }
    }

    private static class StressThread extends Thread {

        private final long deadLine;
        private Exception e;

        StressThread(long deadLine) {
            this.deadLine = deadLine;
        }

        @Override
        public void run() {
            try {
                while (System.currentTimeMillis() < deadLine) {
                    Logger.getLogger(randomName());
                }
            } catch (Exception e) {
                this.e = e;
            }
        }
    }
}
