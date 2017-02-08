package com.hazelcast.spi.impl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.concurrent.CancellationException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class AbstractInvocationFuture_CancelTest extends AbstractInvocationFuture_AbstractTest {

    @Rule
    public final ExpectedException exceptionRule = ExpectedException.none();

    @Test
    public void whenCancelCalled_thenFutureCancelled() {
        // When
        final boolean result = future.cancel(true);

        // Then
        assertTrue(result);
        assertTrue(future.isCancelled());
        assertTrue(future.isDone());
    }

    @Test
    public void whenCancelled_thenCantCancelAgain() {
        // When
        future.cancel(true);

        // Then
        assertFalse(future.cancel(true));
    }

    @Test
    public void whenCancelled_thenGetThrowsCancelled() throws Exception {
        // Given
        future.cancel(true);

        // Then
        exceptionRule.expect(CancellationException.class);

        // When
        future.get();
    }

    @Test
    public void whenCancelled_thenJoinThrowsCancelled() {
        // Given
        future.cancel(true);

        // Then
        exceptionRule.expect(CancellationException.class);

        // When
        future.join();
    }

    @Test
    public void whenCancelled_thenCompleteNoEffect() {
        // Given
        future.cancel(true);

        // When
        future.complete(value);

        // Then
        exceptionRule.expect(CancellationException.class);
        future.join();
    }
}
