package com.hazelcast.spi.impl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class AbstractInvocationFuture_CancelTest extends AbstractInvocationFuture_AbstractTest {

    @Test
    public void whenCalled_thenIgnored() {
        assertFalse(future.cancel(true));
        assertFalse(future.isCancelled());

        future.complete(value);
        assertSame(value, future.join());
    }
}
