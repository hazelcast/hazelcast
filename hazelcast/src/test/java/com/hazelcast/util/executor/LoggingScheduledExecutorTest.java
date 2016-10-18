/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util.executor;

import com.hazelcast.logging.AbstractLogger;
import com.hazelcast.logging.LogEvent;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

import static junit.framework.TestCase.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class LoggingScheduledExecutorTest extends HazelcastTestSupport {

    @Test
    public void logs_task_execution_exception() throws Exception {
        final TestLogger logger = new TestLogger();
        ScheduledExecutorService executor = new LoggingScheduledExecutor(logger, 1, new TestThreadFactory());
        executor.execute(new FailedRunnable());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertInstanceOf(RuntimeException.class, logger.getThrowable());
                String message = logger.getMessage();
                assertTrue("Found message: '" + message + "'", message.contains("FailedRunnable"));
            }
        });
    }

    class FailedRunnable implements Runnable {
        @Override
        public void run() {
            throw new RuntimeException();
        }

        @Override
        public String toString() {
            return "FailedRunnable{}";
        }
    }

    class TestThreadFactory implements ThreadFactory {
        public Thread newThread(Runnable r) {
            return new Thread(r);
        }
    }

    class TestLogger extends AbstractLogger {

        private final AtomicReference<Throwable> throwableHolder = new AtomicReference<Throwable>();
        private final AtomicReference<String> messageHolder = new AtomicReference<String>();

        @Override
        public void log(Level level, String message) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void log(Level level, String message, Throwable thrown) {
            throwableHolder.set(thrown);
            messageHolder.set(message);
        }

        @Override
        public void log(LogEvent logEvent) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Level getLevel() {
            return null;
        }

        @Override
        public boolean isLoggable(Level level) {
            return false;
        }

        public Throwable getThrowable() {
            return throwableHolder.get();
        }

        public String getMessage() {
            return messageHolder.get();
        }
    }
}