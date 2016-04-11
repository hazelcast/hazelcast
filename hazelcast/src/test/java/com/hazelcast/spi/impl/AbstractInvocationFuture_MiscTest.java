package com.hazelcast.spi.impl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class AbstractInvocationFuture_MiscTest extends AbstractInvocationFuture_AbstractTest {

    @Test
    public void toString_whenNotCompleted() {
        String s = future.toString();
        assertEquals("InvocationFuture{invocation=someinvocation, done=false}", s);
    }

    @Test
    public void toString_whenCompleted() {
        future.complete(value);
        String s = future.toString();
        assertEquals("InvocationFuture{invocation=someinvocation, value=somevalue}", s);
    }
}
