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

import java.util.logging.Level;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NoLoggerTest extends AbstractLoggerTest {

    private ILogger logger;

    @Before
    public void setUp() {
        logger = new NoLogFactory().getLogger("NoLoggerTest");
    }

    @Test
    public void finest_withMessage() {
        logger.finest(MESSAGE);
    }

    @Test
    public void finest_withThrowable() {
        logger.finest(THROWABLE);
    }

    @Test
    public void finest() {
        logger.finest(MESSAGE, THROWABLE);
    }

    @Test
    public void isFinestEnabled() {
        assertFalse(logger.isFinestEnabled());
    }

    @Test
    public void fine_withMessage() {
        logger.fine(MESSAGE);
    }

    @Test
    public void fine_withThrowable() {
        logger.fine(THROWABLE);
    }

    @Test
    public void fine() {
        logger.fine(MESSAGE, THROWABLE);
    }

    @Test
    public void isFineEnabled() {
        assertFalse(logger.isFineEnabled());
    }

    @Test
    public void info() {
        logger.info(MESSAGE, THROWABLE);
    }

    @Test
    public void info_withMessage() {
        logger.info(MESSAGE);
    }

    @Test
    public void info_withThrowable() {
        logger.info(THROWABLE);
    }

    @Test
    public void isInfoEnabled() {
        assertFalse(logger.isInfoEnabled());
    }

    @Test
    public void warning_withMessage() {
        logger.warning(MESSAGE);
    }

    @Test
    public void warning_withThrowable() {
        logger.warning(THROWABLE);
    }

    @Test
    public void warning() {
        logger.warning(MESSAGE, THROWABLE);
    }

    @Test
    public void isWarningEnabled() {
        assertFalse(logger.isWarningEnabled());
    }

    @Test
    public void severe_withMessage() {
        logger.severe(MESSAGE);
    }

    @Test
    public void severe_withThrowable() {
        logger.severe(THROWABLE);
    }

    @Test
    public void severe() {
        logger.severe(MESSAGE, THROWABLE);
    }

    @Test
    public void isSevereEnabled() {
        assertFalse(logger.isSevereEnabled());
    }

    @Test
    public void log_withMessage() {
        logger.log(Level.OFF, MESSAGE);
        logger.log(Level.FINEST, MESSAGE);
        logger.log(Level.FINER, MESSAGE);
        logger.log(Level.FINE, MESSAGE);
        logger.log(Level.INFO, MESSAGE);
        logger.log(Level.WARNING, MESSAGE);
        logger.log(Level.SEVERE, MESSAGE);
        logger.log(Level.ALL, MESSAGE);
    }

    @Test
    public void log() {
        logger.log(Level.OFF, MESSAGE, THROWABLE);
        logger.log(Level.FINEST, MESSAGE, THROWABLE);
        logger.log(Level.FINER, MESSAGE, THROWABLE);
        logger.log(Level.FINE, MESSAGE, THROWABLE);
        logger.log(Level.INFO, MESSAGE, THROWABLE);
        logger.log(Level.WARNING, MESSAGE, THROWABLE);
        logger.log(Level.SEVERE, MESSAGE, THROWABLE);
        logger.log(Level.ALL, MESSAGE, THROWABLE);
    }

    @Test
    public void logEvent() {
        logger.log(LOG_EVENT);
    }

    @Test
    public void getLevel() {
        assertEquals(Level.OFF, logger.getLevel());
    }

    @Test
    public void isLoggable() {
        assertFalse(logger.isLoggable(Level.OFF));
        assertFalse(logger.isLoggable(Level.FINEST));
        assertFalse(logger.isLoggable(Level.FINER));
        assertFalse(logger.isLoggable(Level.FINE));
        assertFalse(logger.isLoggable(Level.INFO));
        assertFalse(logger.isLoggable(Level.WARNING));
        assertFalse(logger.isLoggable(Level.SEVERE));
        assertFalse(logger.isLoggable(Level.ALL));
    }
}
