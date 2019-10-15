/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.test.HazelcastTestSupport;

import java.util.logging.Level;
import java.util.logging.LogRecord;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

abstract class AbstractLoggerTest extends HazelcastTestSupport {

    static final String MESSAGE = "Any message";
    static final Throwable THROWABLE = new Exception("expected exception");

    static final LogEvent LOG_EVENT;
    static final LogEvent LOG_EVENT_OFF;

    static {
        LogRecord logRecord = new LogRecord(Level.WARNING, MESSAGE);
        logRecord.setThrown(THROWABLE);
        logRecord.setLoggerName("AbstractLogFactoryTest");

        LogRecord logRecordOff = new LogRecord(Level.OFF, MESSAGE);
        logRecordOff.setThrown(THROWABLE);
        logRecordOff.setLoggerName("AbstractLogFactoryTest");

        Member member = mock(Member.class);

        LOG_EVENT = new LogEvent(spy(logRecord), member);
        LOG_EVENT_OFF = new LogEvent(spy(logRecordOff), member);
    }
}
