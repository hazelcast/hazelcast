/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.test.annotation.QuickTest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.test.HazelcastTestSupport.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class LogEntryMatcherTest {
    private static final String TEST_NAME_CONTEXT_KEY = "test-name";
    private static final Logger LOGGER = LogManager.getLogger(LogEntryMatcherTest.class);

    @Test
    public void matchesLogEventFromCurrentTestContext() {
        String message = message("current-context");

        try (LogEntryMatcher matcher = installMessageMatcher(message)) {
            LOGGER.error(message);
            assertTrue(matcher.matched());
        }
    }

    @Test
    public void throwsWhenPredicateIsNull() {
        assertThrows(NullPointerException.class, () -> LogEntryMatcher.install(null));
    }

    @Test
    public void ignoresLogEventFromDifferentTestContext() {
        AtomicInteger predicateInvocations = new AtomicInteger();

        try (LogEntryMatcher matcher = LogEntryMatcher.install(event -> {
            predicateInvocations.incrementAndGet();
            return true;
        })) {
            runWithLog4jTestName("different-test", () -> LOGGER.error(message("different-context")));

            assertFalse(matcher.matched());
            assertEquals(0, predicateInvocations.get());
        }
    }

    @Test
    public void stopsObservingLogEventsAfterClose() {
        AtomicInteger predicateInvocations = new AtomicInteger();
        LogEntryMatcher matcher = LogEntryMatcher.install(event -> {
            predicateInvocations.incrementAndGet();
            return true;
        });

        matcher.close();
        LOGGER.error(message("after-close"));

        assertFalse(matcher.matched());
        assertEquals(0, predicateInvocations.get());
    }

    @Test
    public void supportsMultipleActiveMatchers() {
        String firstMessage = message("first");
        String secondMessage = message("second");

        try (LogEntryMatcher first = installMessageMatcher(firstMessage);
             LogEntryMatcher second = installMessageMatcher(secondMessage)) {
            LOGGER.error(firstMessage);
            LOGGER.error(secondMessage);

            assertTrue(first.matched());
            assertTrue(second.matched());
        }
    }

    private static String message(String value) {
        return LogEntryMatcherTest.class.getSimpleName() + '-' + value + '-' + System.nanoTime();
    }

    private static LogEntryMatcher installMessageMatcher(String message) {
        return LogEntryMatcher.install(event -> message.equals(event.getMessage().getFormattedMessage()));
    }

    private static void runWithLog4jTestName(String testName, Runnable task) {
        String previousTestName = ThreadContext.get(TEST_NAME_CONTEXT_KEY);
        ThreadContext.put(TEST_NAME_CONTEXT_KEY, testName);
        try {
            task.run();
        } finally {
            if (previousTestName == null) {
                ThreadContext.remove(TEST_NAME_CONTEXT_KEY);
            } else {
                ThreadContext.put(TEST_NAME_CONTEXT_KEY, previousTestName);
            }
        }
    }
}
