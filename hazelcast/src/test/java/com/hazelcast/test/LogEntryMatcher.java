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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

/**
 * Installs a temporary Log4j2 root appender and records whether any JVM log event matches a predicate.
 * <p>
 * The appender is attached to the root logger, so it observes log events at JVM level, including logs emitted by
 * unrelated tests running in the same JVM. Events are filtered to the current Hazelcast runner {@code test-name}
 * logging context before the predicate is evaluated, but this is still JVM-level logging state scoped by test name.
 */
public final class LogEntryMatcher implements AutoCloseable {
    private static final String TEST_NAME_CONTEXT_KEY = "test-name";
    private static final AtomicLong APPENDER_ID = new AtomicLong();

    private final Configuration logging;
    private final PredicateAppender appender;

    private LogEntryMatcher(Configuration logging, PredicateAppender appender) {
        this.logging = logging;
        this.appender = appender;
    }

    public static LogEntryMatcher install(Predicate<LogEvent> predicate) {
        Predicate<LogEvent> validatedPredicate = requireNonNull(predicate, "predicate");
        String testName = currentTestName();
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        Configuration logging = context.getConfiguration();
        PredicateAppender appender = new PredicateAppender(testName, validatedPredicate);
        appender.start();
        logging.addAppender(appender);
        logging.getRootLogger().addAppender(appender, null, null);
        return new LogEntryMatcher(logging, appender);
    }

    public boolean matched() {
        return appender.matched.get();
    }

    @Override
    public void close() {
        logging.getRootLogger().removeAppender(appender.getName());
        logging.getAppenders().remove(appender.getName());
        appender.stop();
    }

    private static String currentTestName() {
        String testName = TestNameHolder.getTestMethodName();
        if (testName == null) {
            throw new IllegalStateException("LogEntryMatcher requires the current Hazelcast runner test name context");
        }
        return testName;
    }

    private static final class PredicateAppender extends AbstractAppender {
        private final String testName;
        private final Predicate<LogEvent> predicate;
        private final AtomicBoolean matched = new AtomicBoolean();

        private PredicateAppender(String testName, Predicate<LogEvent> predicate) {
            super(PredicateAppender.class.getName() + "-" + APPENDER_ID.incrementAndGet(), null, null, true, null);
            this.testName = testName;
            this.predicate = predicate;
        }

        @Override
        public void append(LogEvent event) {
            if (!matched.get()
                    && testName.equals(event.getContextData().getValue(TEST_NAME_CONTEXT_KEY))
                    && predicate.test(event)) {
                matched.set(true);
            }
        }
    }
}
