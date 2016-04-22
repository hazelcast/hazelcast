package com.hazelcast.spi.impl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertSame;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class AbstractInvocationFuture_CompleteTest extends AbstractInvocationFuture_AbstractTest {

    @Test
    public void whenAlreadyCompleted_thenIgnored(){
        future.complete(value);

        Object newValue = new Object();
        future.complete(newValue);

        assertSame(value, future.getState());
    }
}
