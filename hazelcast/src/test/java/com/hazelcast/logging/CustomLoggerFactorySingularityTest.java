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
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class CustomLoggerFactorySingularityTest extends HazelcastTestSupport {

    private static final int TEST_DURATION_SECONDS = 5;
    private static final int THREAD_COUNT = 4;

    private static final String LOGGING_CLASS_PROP_NAME = "hazelcast.logging.class";

    private static final Field LOGGER_FACTORY_FIELD = getField("loggerFactory");
    private static final Field FACTORY_LOCK_FIELD = getField("FACTORY_LOCK");

    private String originalLoggingClass;
    private LoggerFactory originalLoggerFactory;

    @Before
    public void setUp() throws Exception {
        originalLoggingClass = System.getProperty(LOGGING_CLASS_PROP_NAME);
        originalLoggerFactory = (LoggerFactory) LOGGER_FACTORY_FIELD.get(null);
        LOGGER_FACTORY_FIELD.set(null, null);
    }

    @After
    public void tearDown() throws Exception {
        restoreProperty(LOGGING_CLASS_PROP_NAME, originalLoggingClass);
        LOGGER_FACTORY_FIELD.set(null, originalLoggerFactory);
    }

    @Test
    public void testCustomLoggerFactorySingularity() throws Exception {
        System.setProperty(LOGGING_CLASS_PROP_NAME, CustomLoggerFactory.class.getName());

        long deadLine = System.currentTimeMillis() + SECONDS.toMillis(TEST_DURATION_SECONDS);
        TestThread[] threads = startThreads(deadLine);

        while (System.currentTimeMillis() < deadLine) {
            if (CustomLoggerFactory.singularityCheckFailed) {
                fail("singularity check failed");
            }

            // begin the new round of a factory creation
            synchronized (FACTORY_LOCK_FIELD.get(null)) {
                CustomLoggerFactory.INSTANCE_COUNT.set(0);
                LOGGER_FACTORY_FIELD.set(null, null);
            }
        }

        assertThreadsEventuallyFinishesWithoutException(threads);
    }

    private static Field getField(String name) {
        Field field;
        try {
            field = Logger.class.getDeclaredField(name);
        } catch (NoSuchFieldException e) {
            throw new AssertionError(name + " field not found");
        }
        field.setAccessible(true);
        return field;
    }

    private static void restoreProperty(String name, String value) {
        if (value == null) {
            System.clearProperty(name);
        } else {
            System.setProperty(name, value);
        }
    }

    private static TestThread[] startThreads(long deadLine) {
        TestThread[] threads = new TestThread[THREAD_COUNT];
        for (int i = 0; i < THREAD_COUNT; i++) {
            threads[i] = new TestThread(deadLine);
        }
        for (int i = 0; i < THREAD_COUNT; i++) {
            threads[i].start();
        }
        return threads;
    }

    private static void assertThreadsEventuallyFinishesWithoutException(TestThread[] threads) throws Exception {
        for (TestThread thread : threads) {
            thread.join();
            Exception exception = thread.e;
            if (exception != null) {
                throw exception;
            }
        }
    }

    private static class CustomLoggerFactory implements LoggerFactory {

        public static final AtomicLong INSTANCE_COUNT = new AtomicLong();

        public static volatile boolean singularityCheckFailed = false;

        public CustomLoggerFactory() {
            final long count = INSTANCE_COUNT.incrementAndGet();

            // XXX: All exceptions during factory construction are inhibited in Logger.tryCreateLoggerFactory(), so we use a flag
            // here to pass the status to the main thread instead of an assertion.
            if (count != 1) {
                singularityCheckFailed = true;
            }
        }

        @Override
        public ILogger getLogger(String name) {
            return mock(ILogger.class, withSettings().stubOnly());
        }
    }

    private static class TestThread extends Thread {

        private final long deadLine;
        private Exception e;

        TestThread(long deadLine) {
            this.deadLine = deadLine;
        }

        @Override
        public void run() {
            try {
                while (System.currentTimeMillis() < deadLine) {
                    Logger.getLogger(randomName());
                    assertInstanceOf(CustomLoggerFactory.class, Logger.newLoggerFactory("irrelevant"));
                }
            } catch (Exception e) {
                this.e = e;
            }
        }
    }
}
