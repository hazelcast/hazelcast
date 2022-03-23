package com.hazelcast.jet.impl.execution.init;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IntArrayWithPositionTest {
    private static final int TEST_ARRAY_SIZE = 20;

    @Test
    public void when_elementsAdded_then_properSizeArray() {
        IntArrayWithPosition intArrayWithPosition = new IntArrayWithPosition(TEST_ARRAY_SIZE);
        for (int i = 0; i < TEST_ARRAY_SIZE; i++) {
            assertEquals(i, intArrayWithPosition.getFilledElements().length);
            intArrayWithPosition.add(1);
        }
        assertEquals(TEST_ARRAY_SIZE, intArrayWithPosition.getFilledElements().length);
    }

    @Test
    public void when_elementsAdded_then_properElementsInArray() {
        IntArrayWithPosition intArrayWithPosition = new IntArrayWithPosition(TEST_ARRAY_SIZE);
        for (int i = 0; i < TEST_ARRAY_SIZE; i++) {
            intArrayWithPosition.add(i);
            int[] filledElements = intArrayWithPosition.getFilledElements();
            for (int j = 0; j <= i; j++) {
                assertEquals(j, filledElements[j]);
            }
        }
    }
}