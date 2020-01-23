/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import java.util.logging.Level;
import java.util.logging.LogRecord;

import static org.apache.log4j.Level.DEBUG;
import static org.apache.log4j.Level.ERROR;
import static org.apache.log4j.Level.INFO;
import static org.apache.log4j.Level.OFF;
import static org.apache.log4j.Level.TRACE;
import static org.apache.log4j.Level.WARN;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class Log4jLoggerTest extends AbstractLoggerTest {

    private org.apache.log4j.Logger mockLogger;
    private ILogger hazelcastLogger;

    @Before
    public void setUp() {
        mockLogger = mock(org.apache.log4j.Logger.class);
        hazelcastLogger = new Log4jFactory.Log4jLogger(mockLogger);
    }

    @Test
    public void isLoggable_whenLevelOff_shouldReturnFalse() {
        assertFalse(hazelcastLogger.isLoggable(Level.OFF));
    }

    @Test
    public void logWithThrowable_shouldCallLogWithThrowable() {
        hazelcastLogger.warning(MESSAGE, THROWABLE);
        verify(mockLogger, times(1)).log(WARN, MESSAGE, THROWABLE);
    }

    @Test
    public void logAtLevelOff_shouldLogAtLevelOff() {
        hazelcastLogger.log(Level.OFF, MESSAGE);
        verify(mockLogger, times(1)).log(OFF, MESSAGE);
    }

    @Test
    public void logFinest_shouldLogTrace() {
        hazelcastLogger.finest(MESSAGE);
        verify(mockLogger, times(1)).log(TRACE, MESSAGE);
    }

    @Test
    public void logFiner_shouldLogDebug() {
        hazelcastLogger.log(Level.FINER, MESSAGE);
        verify(mockLogger, times(1)).log(DEBUG, MESSAGE);
    }

    @Test
    public void logFine_shouldLogDebug() {
        hazelcastLogger.fine(MESSAGE);
        verify(mockLogger, times(1)).log(DEBUG, MESSAGE);
    }

    @Test
    public void logInfo_shouldLogInfo() {
        hazelcastLogger.info(MESSAGE);
        verify(mockLogger, times(1)).log(INFO, MESSAGE);
    }

    @Test
    public void logWarning_shouldLogWarn() {
        hazelcastLogger.warning(MESSAGE);
        verify(mockLogger, times(1)).log(WARN, MESSAGE);
    }

    @Test
    public void logSevere_shouldLogError() {
        hazelcastLogger.severe(MESSAGE);
        verify(mockLogger, times(1)).log(ERROR, MESSAGE);
    }

    @Test
    @SuppressWarnings("ThrowableNotThrown")
    public void logEvent_shouldLog() {
        LogRecord logRecord = LOG_EVENT.getLogRecord();

        hazelcastLogger.log(LOG_EVENT);

        verifyZeroInteractions(mockLogger);
        verify(logRecord, atLeastOnce()).getLevel();
        verify(logRecord, atLeastOnce()).getLoggerName();
        verify(logRecord, atLeastOnce()).getMessage();
        verify(logRecord, atLeastOnce()).getThrown();
        verifyNoMoreInteractions(logRecord);
    }

    @Test
    public void logEvent_withLogLevelOff_shouldNotLog() {
        LogRecord logRecord = LOG_EVENT_OFF.getLogRecord();

        hazelcastLogger.log(LOG_EVENT_OFF);

        verifyZeroInteractions(mockLogger);
        verify(logRecord, atLeastOnce()).getLevel();
        verifyNoMoreInteractions(logRecord);
    }
}
