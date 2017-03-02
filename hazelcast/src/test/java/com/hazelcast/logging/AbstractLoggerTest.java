package com.hazelcast.logging;

import com.hazelcast.core.Member;
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
