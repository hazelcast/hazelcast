package com.hazelcast.topic;

import com.hazelcast.core.MessageListener;
import com.hazelcast.ringbuffer.StaleSequenceException;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class TerminatingTopicErrorHandlerTest {

    @Test
    public void test() {
        test(new RuntimeException());
        test(new Exception());
        test(new StaleSequenceException("f", 1));
        test(new OutOfMemoryError());
    }

    public void test(Throwable throwable) {
        TerminatingTopicErrorHandler errorHandler = new TerminatingTopicErrorHandler();
        boolean result = errorHandler.terminate(throwable, mock(MessageListener.class));
        assertTrue(result);
    }
}
