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

import com.hazelcast.cluster.Member;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.logging.Level;
import java.util.logging.LogRecord;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LogEventTest {

    private static final String MESSAGE = "Any message";
    private static final Throwable THROWABLE = new Exception("expected exception");

    private LogRecord logRecord;
    private Member member;

    private LogEvent logEvent;

    @Before
    public void setUp() {
        logRecord = new LogRecord(Level.WARNING, MESSAGE);
        logRecord.setThrown(THROWABLE);
        logRecord.setLoggerName("AbstractLogFactoryTest");
        logRecord.setMillis(23);

        member = mock(Member.class);

        logEvent = new LogEvent(logRecord, member);
    }


    @Test
    public void testGetMember() {
        assertEquals(member, logEvent.getMember());
    }

    @Test
    public void testGetLogRecord() {
        assertEquals(logRecord, logEvent.getLogRecord());
    }
}
