package com.hazelcast.nio.tcp.nonblocking;

import com.hazelcast.nio.tcp.nonblocking.SelectorOptimizer.IteratorImpl;
import com.hazelcast.nio.tcp.nonblocking.SelectorOptimizer.SelectionKeys;
import com.hazelcast.nio.tcp.nonblocking.SelectorOptimizer.SelectionKeysSet;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.channels.SelectionKey;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class SelectionKeysSetTest extends HazelcastTestSupport {

    private final SelectionKey key1 = mock(SelectionKey.class);
    private final SelectionKey key2 = mock(SelectionKey.class);
    private final SelectionKey key3 = mock(SelectionKey.class);
    private SelectionKeysSet selectionKeysSet;

    @Before
    public void setup() {
        selectionKeysSet = new SelectionKeysSet();
    }

    @Test
    public void remove_doesNothing() {
        assertFalse(selectionKeysSet.remove(key1));
    }

    @Test
    public void contains_doesNothing() {
        assertFalse(selectionKeysSet.contains(key1));
    }

    @Test
    public void iteratorRecycled() {
        Iterator it1 = selectionKeysSet.iterator();
        Iterator it2 = selectionKeysSet.iterator();
        assertSame(it1, it2);
    }

    @Test
    public void add_whenCapacityNotSufficient() {
        List<SelectionKey> expectedKeys = new LinkedList<SelectionKey>();
        for (int k = 0; k < SelectionKeys.INITIAL_CAPACITY * 4; k++) {
            SelectionKey key = mock(SelectionKey.class);
            expectedKeys.add(key);

            selectionKeysSet.add(key);
            assertEquals(expectedKeys.size(), selectionKeysSet.size());
        }

        SelectionKeys active = selectionKeysSet.activeKeys;
        assertEquals(active.size, expectedKeys.size());
        for (int k = 0; k < expectedKeys.size(); k++) {
            SelectionKey expected = expectedKeys.get(k);
            SelectionKey found = active.keys[k];
            assertSame(expected, found);
        }
    }

    @Test
    public void add_whenNull() {
        boolean result = selectionKeysSet.add(null);

        assertFalse(result);
        SelectionKeys active = selectionKeysSet.activeKeys;
        assertEquals(0, active.size);
    }

    // tests the regular nio loop; hasNext, next, remove.
    @Test
    public void testLoop() {
        List<SelectionKey> addedKeys = Arrays.asList(key1, key2, key3);

        for (SelectionKey selectionKey : addedKeys) {
            selectionKeysSet.add(selectionKey);
        }

        IteratorImpl it = (IteratorImpl) selectionKeysSet.iterator();
        int k = 0;
        for (SelectionKey expected : addedKeys) {
            // check if the hasNext returns true
            boolean hasNext = it.hasNext();
            assertTrue(hasNext);

            // check if the key is correct.
            SelectionKey next = it.next();
            assertSame(expected, next);
            assertEquals(k, it.index);

            // do the remove and check if the slot is nulled
            it.remove();
            assertNull(it.keys[it.index]);
            k++;
        }

        assertFalse(it.hasNext());
    }

    @Test(expected = NoSuchElementException.class)
    public void next_whenNoItem() {
        IteratorImpl it = (IteratorImpl) selectionKeysSet.iterator();

        it.next();
    }

    @Test(expected = IllegalStateException.class)
    public void remove_whenNoItem() {
        IteratorImpl it = (IteratorImpl) selectionKeysSet.iterator();

        it.remove();
    }
}
