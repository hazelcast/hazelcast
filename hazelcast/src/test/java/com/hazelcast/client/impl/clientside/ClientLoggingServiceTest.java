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

package com.hazelcast.client.impl.clientside;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LogEvent;
import com.hazelcast.logging.LogListener;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.logging.Level;

import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientLoggingServiceTest extends HazelcastTestSupport {

    private TestHazelcastFactory factory;
    private LoggingService loggingService;
    private LogListener logListener;
    private LogEvent logEvent;

    @Before
    public void setUp() {
        factory = new TestHazelcastFactory();

        HazelcastInstance member = factory.newHazelcastInstance();
        HazelcastInstance client = factory.newHazelcastClient();

        loggingService = client.getLoggingService();

        logListener = new LogListener() {
            @Override
            public void log(LogEvent logEvent) {
            }
        };

        logEvent = new LogEvent(null, member.getCluster().getLocalMember());
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddLogListener() {
        loggingService.addLogListener(Level.INFO, logListener);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemoveLogListener() {
        loggingService.removeLogListener(logListener);
    }

    @Test
    public void testLog_whenLogEvent_thenNothingHappens() {
        ILogger logger = loggingService.getLogger("test");
        logger.log(logEvent);
    }

    @Test
    public void testLog_whenGetLevel_thenDefaultLevelIsReturned() {
        ILogger logger = loggingService.getLogger("test");
        assertNotNull(logger.getLevel());
    }
}
