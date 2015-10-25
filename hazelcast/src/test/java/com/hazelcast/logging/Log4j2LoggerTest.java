/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.logging.log4j.spi.ExtendedLogger;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.logging.Level;

import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class Log4j2LoggerTest {
    private static final String LOGGER_NAME = Log4j2Factory.Log4j2Logger.class.getName();

    private ExtendedLogger mockLogger;
    private ILogger hazelcastLogger;

    @Before
    public void setUp() {
        mockLogger = mock(ExtendedLogger.class);
        hazelcastLogger = new Log4j2Factory.Log4j2Logger(mockLogger);
    }

    @Test
    public void isLoggable_whenLevelOff_shouldReturnFalse() {
        assertFalse(hazelcastLogger.isLoggable(Level.OFF));
    }

    @Test
    public void logWithThrowable_shouldCallLogWithThrowable() {
        final Exception thrown = new Exception();
        hazelcastLogger.warning("message", thrown);
        verify(mockLogger, times(1)).logIfEnabled(
                LOGGER_NAME, org.apache.logging.log4j.Level.WARN, null, "message", thrown);
    }

    @Test
    public void logAtLevelOff_shouldLogAtLevelOff() {
        hazelcastLogger.log(Level.OFF, "message");
        verify(mockLogger, times(1)).logIfEnabled(LOGGER_NAME, org.apache.logging.log4j.Level.OFF, null, "message");
    }

    @Test
    public void logFinest_shouldLogTrace() {
        hazelcastLogger.finest("message");
        verify(mockLogger, times(1)).logIfEnabled(LOGGER_NAME, org.apache.logging.log4j.Level.TRACE, null, "message");
    }

    @Test
    public void logFine_shouldLogDebug() {
        hazelcastLogger.fine("message");
        verify(mockLogger, times(1)).logIfEnabled(LOGGER_NAME, org.apache.logging.log4j.Level.DEBUG, null, "message");
    }

    @Test
    public void logInfo_shouldLogInfo() {
        hazelcastLogger.info("message");
        verify(mockLogger, times(1)).logIfEnabled(LOGGER_NAME, org.apache.logging.log4j.Level.INFO, null, "message");
    }

    @Test
    public void logWarning_shouldLogWarn() {
        hazelcastLogger.warning("message");
        verify(mockLogger, times(1)).logIfEnabled(LOGGER_NAME, org.apache.logging.log4j.Level.WARN, null, "message");
    }

    @Test
    public void logSevere_shouldLogError() {
        hazelcastLogger.severe("message");
        verify(mockLogger, times(1)).logIfEnabled(LOGGER_NAME, org.apache.logging.log4j.Level.ERROR, null, "message");
    }
}
