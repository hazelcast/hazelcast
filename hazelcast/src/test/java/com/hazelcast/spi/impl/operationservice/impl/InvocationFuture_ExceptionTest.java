package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.PrintWriter;
import java.io.StringWriter;

import static com.hazelcast.util.ExceptionUtil.EXCEPTION_SEPARATOR;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class InvocationFuture_ExceptionTest extends HazelcastTestSupport {

    @Test
    public void testExceptionIsCloned() {
        HazelcastInstance hz = createHazelcastInstance();
        final ExpectedRuntimeException original = new ExpectedRuntimeException("foobar");
        Operation op = new DummyOperation(new Runnable() {
            @Override
            public void run() {
                throw original;
            }
        });

        InternalCompletableFuture<Object> f = getOperationService(hz).invokeOnPartition(null, op, 1);

        ExpectedRuntimeException e1 = getExpectedRuntimeException(f);
        ExpectedRuntimeException e2 = getExpectedRuntimeException(f);
        assertNotSame(e1, e2);
        assertNotSame(original, e1);
        assertNotSame(original, e2);
    }

    @Test
    public void testStacktraceAlwaysFixed() {
        HazelcastInstance hz = createHazelcastInstance();
        final ExpectedRuntimeException original = new ExpectedRuntimeException("foobar");
        Operation op = new DummyOperation(new Runnable() {
            @Override
            public void run() {
                throw original;
            }
        });

        InternalCompletableFuture<Object> f = getOperationService(hz).invokeOnPartition(null, op, 1);

        ExpectedRuntimeException found = getExpectedRuntimeException(f);
        assertTrue(stacktraceToString(found).contains(EXCEPTION_SEPARATOR));
        assertFalse(stacktraceToString(original).contains(EXCEPTION_SEPARATOR));
    }

    private String stacktraceToString(Throwable t) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        t.printStackTrace(pw);
        return sw.toString();
    }

    private ExpectedRuntimeException getExpectedRuntimeException(InternalCompletableFuture<Object> f) {
        try {
            f.getSafely();
            fail();
            // won't get executed
            return null;
        } catch (ExpectedRuntimeException e) {
            return e;
        }
    }
}
