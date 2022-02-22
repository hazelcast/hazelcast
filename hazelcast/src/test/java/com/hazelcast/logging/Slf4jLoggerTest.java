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

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;

import java.util.logging.Level;

import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class Slf4jLoggerTest extends AbstractLoggerTest {

    private Logger mockLogger;
    private ILogger hazelcastLogger;

    @Before
    public void setUp() {
        mockLogger = mock(Logger.class);
        hazelcastLogger = new Slf4jFactory.Slf4jLogger(mockLogger);
    }

    @Test
    public void isLoggable_whenLevelOff_shouldReturnFalse() {
        assertFalse(hazelcastLogger.isLoggable(Level.OFF));
    }

    @Test
    public void logWithThrowable_shouldCallLogWithThrowable() {
        hazelcastLogger.warning(MESSAGE, THROWABLE);
        verify(mockLogger, times(1)).warn(MESSAGE, THROWABLE);
    }

    @Test
    public void logAtLevelOff_shouldNotLog() {
        hazelcastLogger.log(Level.OFF, MESSAGE);
        verifyZeroInteractions(mockLogger);
    }

    @Test
    public void logAtLevelOff_shouldNotLog_withThrowable() {
        hazelcastLogger.log(Level.OFF, MESSAGE, THROWABLE);
        verifyZeroInteractions(mockLogger);
    }

    @Test
    public void logAtLevelAll_shouldLogInfo() {
        hazelcastLogger.log(Level.ALL, MESSAGE);
        verify(mockLogger, times(1)).info(MESSAGE);
    }

    @Test
    public void logAtLevelAll_shouldLogInfo_withThrowable() {
        hazelcastLogger.log(Level.ALL, MESSAGE, THROWABLE);
        verify(mockLogger, times(1)).info(MESSAGE, THROWABLE);
    }

    @Test
    public void logFinest_shouldLogTrace() {
        hazelcastLogger.finest(MESSAGE);
        verify(mockLogger, times(1)).trace(MESSAGE);
    }

    @Test
    public void logFinest_shouldLogTrace_withThrowable() {
        hazelcastLogger.finest(MESSAGE, THROWABLE);
        verify(mockLogger, times(1)).trace(MESSAGE, THROWABLE);
    }

    @Test
    public void logFine_shouldLogDebug() {
        hazelcastLogger.fine(MESSAGE);
        verify(mockLogger, times(1)).debug(MESSAGE);
    }

    @Test
    public void logFine_shouldLogDebug_withThrowable() {
        hazelcastLogger.fine(MESSAGE, THROWABLE);
        verify(mockLogger, times(1)).debug(MESSAGE, THROWABLE);
    }

    @Test
    public void logInfo_shouldLogInfo() {
        hazelcastLogger.info(MESSAGE);
        verify(mockLogger, times(1)).info(MESSAGE);
    }

    @Test
    public void logInfo_shouldLogInfo_withThrowable() {
        hazelcastLogger.log(Level.INFO, MESSAGE, THROWABLE);
        verify(mockLogger, times(1)).info(MESSAGE, THROWABLE);
    }

    @Test
    public void logWarning_shouldLogWarn() {
        hazelcastLogger.warning(MESSAGE);
        verify(mockLogger, times(1)).warn(MESSAGE);
    }

    @Test
    public void logWarning_shouldLogWarn_withThrowable() {
        hazelcastLogger.warning(MESSAGE, THROWABLE);
        verify(mockLogger, times(1)).warn(MESSAGE, THROWABLE);
    }

    @Test
    public void logSevere_shouldLogError() {
        hazelcastLogger.severe(MESSAGE);
        verify(mockLogger, times(1)).error(MESSAGE);
    }

    @Test
    public void logSevere_shouldLogError_withThrowable() {
        hazelcastLogger.severe(MESSAGE, THROWABLE);
        verify(mockLogger, times(1)).error(MESSAGE, THROWABLE);
    }

    @Test
    public void logEvent_shouldLogWithCorrectLevel() {
        hazelcastLogger.log(LOG_EVENT);
        verify(mockLogger, times(1)).warn(MESSAGE, THROWABLE);
    }
}
