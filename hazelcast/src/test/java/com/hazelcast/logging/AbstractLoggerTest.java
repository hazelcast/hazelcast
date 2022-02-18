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
import com.hazelcast.instance.SimpleMemberImpl;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Before;

import javax.annotation.Nonnull;
import java.util.logging.Level;
import java.util.logging.LogRecord;

abstract class AbstractLoggerTest extends HazelcastTestSupport {

    static final String MESSAGE = "Any message";
    static final Throwable THROWABLE = new Exception("expected exception");

    protected LogEvent LOG_EVENT;
    protected LogEvent LOG_EVENT_OFF;

    @Before
    public void setUpAbstract() {
        LOG_EVENT = createLogEvent(Level.WARNING, "loggerWarn");
        LOG_EVENT_OFF = createLogEvent(Level.OFF, "loggerOff");
    }

    @Nonnull
    private static LogEvent createLogEvent(Level level, String loggerName) {
        LogRecord logRecord = new LogRecord(level, MESSAGE);
        logRecord.setThrown(THROWABLE);
        logRecord.setLoggerName(loggerName);
        Member member = new SimpleMemberImpl();
        return new LogEvent(logRecord, member);
    }
}
