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

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.IsolatedLoggingRule;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.IsolatedLoggingRule.LOGGING_TYPE_LOG4J;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class LoggerStressTest extends HazelcastTestSupport {

    private static final int TEST_DURATION_SECONDS = 5;
    private static final int THREAD_COUNT = 4;

    @Rule
    public final IsolatedLoggingRule isolatedLoggingRule = new IsolatedLoggingRule();

    @Test
    public void stressThreadsWithCustomLoggingClass() throws Exception {
        isolatedLoggingRule.setLoggingClass(LoggingFactoryStub.class);

        long deadLine = System.currentTimeMillis() + SECONDS.toMillis(TEST_DURATION_SECONDS);
        StressThread[] threads = startStressThreads(deadLine);
        assertThreadsEventuallyFinishesWithoutException(threads);
    }

    @Test
    public void stressThreadsWithLoggingType() throws Exception {
        isolatedLoggingRule.setLoggingType(LOGGING_TYPE_LOG4J);

        long deadLine = System.currentTimeMillis() + SECONDS.toMillis(TEST_DURATION_SECONDS);
        StressThread[] threads = startStressThreads(deadLine);
        assertThreadsEventuallyFinishesWithoutException(threads);
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
