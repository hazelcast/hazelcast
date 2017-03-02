package com.hazelcast.logging;

import com.hazelcast.core.Member;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
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
@Category({QuickTest.class, ParallelTest.class})
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
