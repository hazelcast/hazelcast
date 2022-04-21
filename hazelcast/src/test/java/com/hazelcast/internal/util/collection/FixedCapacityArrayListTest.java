package com.hazelcast.internal.util.collection;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class FixedCapacityArrayListTest extends TestCase {
    private static final int TEST_ARRAY_SIZE = 20;

    @Test
    public void when_elementsAdded_then_properSizeArray() {
        for (int i = 0; i < TEST_ARRAY_SIZE; i++) {
            FixedCapacityArrayList<Integer> list = new FixedCapacityArrayList<>(Integer.class, TEST_ARRAY_SIZE);
            for (int j = 0; j < i; j++) {
                list.add(1);
            }
            assertEquals(i, list.asArray().length);
        }
    }

    @Test
    public void when_elementsAdded_then_properElementsInArray() {
        for (int i = 0; i < TEST_ARRAY_SIZE; i++) {
            FixedCapacityArrayList<Integer> list = new FixedCapacityArrayList<>(Integer.class, TEST_ARRAY_SIZE);
            for (int j = 0; j < i; j++) {
                list.add(j);
            }
            Integer[] filledElements = list.asArray();
            for (int j = 0; j < i; j++) {
                assertEquals(j, filledElements[j].intValue());
            }
        }
    }

    @Test
    public void when_arrayIsFetched_then_listIsUnusable() {
        for (int i = 0; i < TEST_ARRAY_SIZE; i++) {
            FixedCapacityArrayList<Integer> list = new FixedCapacityArrayList<>(Integer.class, TEST_ARRAY_SIZE);
            for (int j = 0; j < i; j++) {
                list.add(j);
            }
            list.asArray();
            Assert.assertThrows(NullPointerException.class, () -> {
                list.add(1);
            });
        }
    }
}