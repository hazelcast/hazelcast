package com.hazelcast.internal.eviction;

import com.hazelcast.cache.impl.maxsize.MaxSizeChecker;
import com.hazelcast.cache.impl.maxsize.impl.CompositeMaxSizeChecker;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CompositeMaxSizeCheckerTest {

    @Test(expected = IllegalArgumentException.class)
    public void compositionOperatorCannotBeNull() {
        CompositeMaxSizeChecker.newCompositeMaxSizeChecker(
                null,
                mock(MaxSizeChecker.class),
                mock(MaxSizeChecker.class));
    }

    @Test(expected = IllegalArgumentException.class)
    public void maxSizeCheckersCannotBeNull() {
        CompositeMaxSizeChecker.newCompositeMaxSizeChecker(
                CompositeMaxSizeChecker.CompositionOperator.AND,
                null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void maxSizeCheckersCannotBeEmpty() {
        CompositeMaxSizeChecker.newCompositeMaxSizeChecker(
                CompositeMaxSizeChecker.CompositionOperator.AND);
    }

    @Test
    public void resultShouldReturnTrue_whenAllIsTrue_withAndCompositionOperator() {
        MaxSizeChecker maxSizeChecker1ReturnsTrue = mock(MaxSizeChecker.class);
        MaxSizeChecker maxSizeChecker2ReturnsTrue = mock(MaxSizeChecker.class);

        when(maxSizeChecker1ReturnsTrue.isReachedToMaxSize()).thenReturn(true);
        when(maxSizeChecker2ReturnsTrue.isReachedToMaxSize()).thenReturn(true);

        CompositeMaxSizeChecker compositeMaxSizeChecker =
                CompositeMaxSizeChecker.newCompositeMaxSizeChecker(
                        CompositeMaxSizeChecker.CompositionOperator.AND,
                        maxSizeChecker1ReturnsTrue,
                        maxSizeChecker2ReturnsTrue);

        assertTrue(compositeMaxSizeChecker.isReachedToMaxSize());
    }

    @Test
    public void resultShouldReturnFalse_whenAllIsFalse_withAndCompositionOperator() {
        MaxSizeChecker maxSizeChecker1ReturnsFalse = mock(MaxSizeChecker.class);
        MaxSizeChecker maxSizeChecker2ReturnsFalse = mock(MaxSizeChecker.class);

        when(maxSizeChecker1ReturnsFalse.isReachedToMaxSize()).thenReturn(false);
        when(maxSizeChecker2ReturnsFalse.isReachedToMaxSize()).thenReturn(false);

        CompositeMaxSizeChecker compositeMaxSizeChecker =
                CompositeMaxSizeChecker.newCompositeMaxSizeChecker(
                        CompositeMaxSizeChecker.CompositionOperator.AND,
                        maxSizeChecker1ReturnsFalse,
                        maxSizeChecker2ReturnsFalse);

        assertFalse(compositeMaxSizeChecker.isReachedToMaxSize());
    }

    @Test
    public void resultShouldReturnFalse_whenOneIsFalse_withAndCompositionOperator() {
        MaxSizeChecker maxSizeChecker1ReturnsTrue = mock(MaxSizeChecker.class);
        MaxSizeChecker maxSizeChecker2ReturnsFalse = mock(MaxSizeChecker.class);

        when(maxSizeChecker1ReturnsTrue.isReachedToMaxSize()).thenReturn(true);
        when(maxSizeChecker2ReturnsFalse.isReachedToMaxSize()).thenReturn(false);

        CompositeMaxSizeChecker compositeMaxSizeChecker =
                CompositeMaxSizeChecker.newCompositeMaxSizeChecker(
                        CompositeMaxSizeChecker.CompositionOperator.AND,
                        maxSizeChecker1ReturnsTrue,
                        maxSizeChecker2ReturnsFalse);

        assertFalse(compositeMaxSizeChecker.isReachedToMaxSize());
    }

    @Test
    public void resultShouldReturnTrue_whenAllIsTrue_withOrCompositionOperator() {
        MaxSizeChecker maxSizeChecker1ReturnsTrue = mock(MaxSizeChecker.class);
        MaxSizeChecker maxSizeChecker2ReturnsTrue = mock(MaxSizeChecker.class);

        when(maxSizeChecker1ReturnsTrue.isReachedToMaxSize()).thenReturn(true);
        when(maxSizeChecker2ReturnsTrue.isReachedToMaxSize()).thenReturn(true);

        CompositeMaxSizeChecker compositeMaxSizeChecker =
                CompositeMaxSizeChecker.newCompositeMaxSizeChecker(
                        CompositeMaxSizeChecker.CompositionOperator.OR,
                        maxSizeChecker1ReturnsTrue,
                        maxSizeChecker2ReturnsTrue);

        assertTrue(compositeMaxSizeChecker.isReachedToMaxSize());
    }

    @Test
    public void resultShouldReturnFalse_whenAllIsFalse_withOrCompositionOperator() {
        MaxSizeChecker maxSizeChecker1ReturnsFalse = mock(MaxSizeChecker.class);
        MaxSizeChecker maxSizeChecker2ReturnsFalse = mock(MaxSizeChecker.class);

        when(maxSizeChecker1ReturnsFalse.isReachedToMaxSize()).thenReturn(false);
        when(maxSizeChecker2ReturnsFalse.isReachedToMaxSize()).thenReturn(false);

        CompositeMaxSizeChecker compositeMaxSizeChecker =
                CompositeMaxSizeChecker.newCompositeMaxSizeChecker(
                        CompositeMaxSizeChecker.CompositionOperator.OR,
                        maxSizeChecker1ReturnsFalse,
                        maxSizeChecker2ReturnsFalse);

        assertFalse(compositeMaxSizeChecker.isReachedToMaxSize());
    }

    @Test
    public void resultShouldReturnTrue_whenOneIsTrue_withOrCompositionOperator() {
        MaxSizeChecker maxSizeChecker1ReturnsTrue = mock(MaxSizeChecker.class);
        MaxSizeChecker maxSizeChecker2ReturnsFalse = mock(MaxSizeChecker.class);

        when(maxSizeChecker1ReturnsTrue.isReachedToMaxSize()).thenReturn(true);
        when(maxSizeChecker2ReturnsFalse.isReachedToMaxSize()).thenReturn(false);

        CompositeMaxSizeChecker compositeMaxSizeChecker =
                CompositeMaxSizeChecker.newCompositeMaxSizeChecker(
                        CompositeMaxSizeChecker.CompositionOperator.OR,
                        maxSizeChecker1ReturnsTrue,
                        maxSizeChecker2ReturnsFalse);

        assertTrue(compositeMaxSizeChecker.isReachedToMaxSize());
    }

}
