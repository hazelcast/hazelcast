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

package com.hazelcast.logging;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ILoggerOverloadsTest {

    private final List<LogRecord> logRecords = new ArrayList<>();

    private ILogger logger;

    @Before
    public void setUp() {
        java.util.logging.Logger jdkLogger = java.util.logging.Logger.getAnonymousLogger();
        jdkLogger.setLevel(Level.ALL);
        jdkLogger.setUseParentHandlers(false);
        jdkLogger.addHandler(new Handler() {
            @Override
            public void publish(LogRecord record) {
                logRecords.add(record);
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        });
        logger = new StandardLoggerFactory.StandardLogger(jdkLogger);
    }

    @Test
    public void finest_shouldFormatArguments() {
        logger.finest("message %s", 1);
        logger.finest("message %s %s", 1, 2);
        logger.finest("message %s %s %s", 1, 2, 3);
        logger.finest("message %s %s %s %s", 1, 2, 3, 4);
        logger.finest("message %s %s %s %s %s", 1, 2, 3, 4, 5);
        logger.finest("message %s %s %s %s %s %s", 1, 2, 3, 4, 5, 6);

        assertLogRecords(Level.FINEST, null);
    }

    @Test
    public void fine_shouldFormatArguments() {
        logger.fine("message %s", 1);
        logger.fine("message %s %s", 1, 2);
        logger.fine("message %s %s %s", 1, 2, 3);
        logger.fine("message %s %s %s %s", 1, 2, 3, 4);
        logger.fine("message %s %s %s %s %s", 1, 2, 3, 4, 5);
        logger.fine("message %s %s %s %s %s %s", 1, 2, 3, 4, 5, 6);

        assertLogRecords(Level.FINE, null);
    }

    @Test
    public void info_shouldFormatArguments() {
        logger.info("message %s", 1);
        logger.info("message %s %s", 1, 2);
        logger.info("message %s %s %s", 1, 2, 3);
        logger.info("message %s %s %s %s", 1, 2, 3, 4);
        logger.info("message %s %s %s %s %s", 1, 2, 3, 4, 5);
        logger.info("message %s %s %s %s %s %s", 1, 2, 3, 4, 5, 6);

        assertLogRecords(Level.INFO, null);
    }

    @Test
    public void warning_shouldFormatArguments() {
        logger.warning("message %s", 1);
        logger.warning("message %s %s", 1, 2);
        logger.warning("message %s %s %s", 1, 2, 3);
        logger.warning("message %s %s %s %s", 1, 2, 3, 4);
        logger.warning("message %s %s %s %s %s", 1, 2, 3, 4, 5);
        logger.warning("message %s %s %s %s %s %s", 1, 2, 3, 4, 5, 6);

        assertLogRecords(Level.WARNING, null);
    }

    @Test
    public void severe_shouldFormatArguments() {
        logger.severe("message %s", 1);
        logger.severe("message %s %s", 1, 2);
        logger.severe("message %s %s %s", 1, 2, 3);
        logger.severe("message %s %s %s %s", 1, 2, 3, 4);
        logger.severe("message %s %s %s %s %s", 1, 2, 3, 4, 5);
        logger.severe("message %s %s %s %s %s %s", 1, 2, 3, 4, 5, 6);

        assertLogRecords(Level.SEVERE, null);
    }

    @Test
    public void warningWithThrowable_shouldFormatArgumentsAndLogThrowable() {
        Throwable throwable = new IllegalStateException("expected exception");

        logger.warning("message %s", 1, throwable);
        logger.warning("message %s %s", 1, 2, throwable);
        logger.warning("message %s %s %s", 1, 2, 3, throwable);
        logger.warning("message %s %s %s %s", 1, 2, 3, 4, throwable);
        logger.warning("message %s %s %s %s %s", 1, 2, 3, 4, 5, throwable);
        logger.warning("message %s %s %s %s %s %s", 1, 2, 3, 4, 5, 6, throwable);

        assertLogRecords(Level.WARNING, throwable);
    }

    @Test
    public void severeWithThrowable_shouldFormatArgumentsAndLogThrowable() {
        Throwable throwable = new IllegalStateException("expected exception");

        logger.severe("message %s", 1, throwable);
        logger.severe("message %s %s", 1, 2, throwable);
        logger.severe("message %s %s %s", 1, 2, 3, throwable);
        logger.severe("message %s %s %s %s", 1, 2, 3, 4, throwable);
        logger.severe("message %s %s %s %s %s", 1, 2, 3, 4, 5, throwable);
        logger.severe("message %s %s %s %s %s %s", 1, 2, 3, 4, 5, 6, throwable);

        assertLogRecords(Level.SEVERE, throwable);
    }

    private void assertLogRecords(Level expectedLevel, Throwable expectedThrowable) {
        assertThat(logRecords).extracting(LogRecord::getMessage).containsExactly(
                "message 1",
                "message 1 2",
                "message 1 2 3",
                "message 1 2 3 4",
                "message 1 2 3 4 5",
                "message 1 2 3 4 5 6"
        );

        assertThat(logRecords).allSatisfy(record -> {
            assertThat(record.getLevel()).isEqualTo(expectedLevel);
            assertThat(record.getThrown()).isSameAs(expectedThrowable);
        });
    }
}
