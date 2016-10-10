package com.hazelcast.util;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ExceptionUtilTest extends HazelcastTestSupport {

    private final Throwable throwable = new RuntimeException("expected exception");;

    @Test
    public void testConstructor() {
        assertUtilityConstructor(ExceptionUtil.class);
    }

    @Test
    public void testToString() {
        String result = ExceptionUtil.toString(throwable);

        assertTrue(result.contains("RuntimeException"));
        assertTrue(result.contains("expected exception"));
    }

    @Test
    public void testPeel_whenThrowableIsRuntimeException_thenReturnOriginal() {
        RuntimeException result = ExceptionUtil.peel(throwable);

        assertEquals(throwable, result);
    }

    @Test
    public void testPeel_whenThrowableIsExecutionException_thenReturnCause() {
        RuntimeException result = ExceptionUtil.peel(new ExecutionException(throwable));

        assertEquals(throwable, result);
    }

    @Test
    public void testPeel_whenThrowableIsExecutionExceptionWithNullCause_thenReturnHazelcastException() {
        ExecutionException exception = new ExecutionException(null);
        RuntimeException result = ExceptionUtil.peel(exception);

        assertTrue(result instanceof HazelcastException);
        assertEquals(exception, result.getCause());
    }
}
