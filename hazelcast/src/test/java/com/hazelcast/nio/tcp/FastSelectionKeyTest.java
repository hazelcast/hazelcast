package com.hazelcast.nio.tcp;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class FastSelectionKeyTest {

    private final SelectionKey key1 = mock(SelectionKey.class);
    private final SelectionKey key2 = mock(SelectionKey.class);
    private final SelectionKey key3 = mock(SelectionKey.class);
    private FastSelectionKeys fastSelectionKeys;
    private ILogger logger = Logger.getLogger(FastSelectionKeyTest.class);

    @Before
    public void setup() {
        fastSelectionKeys = new FastSelectionKeys();
    }

    @Test
    public void optimize() throws Exception {
        Selector selector = Selector.open();
        Assume.assumeTrue(FastSelectionKeys.findOptimizableSelectorClass(selector) != null);
        FastSelectionKeys keys = FastSelectionKeys.optimize(selector, logger);
        assertNotNull(keys);
    }

    @Test
    public void remove_doesNothing() {
        assertFalse(fastSelectionKeys.remove(key1));
    }

    @Test
    public void contains_doesNothing() {
        assertFalse(fastSelectionKeys.contains(key1));
    }

    @Test
    public void add_whenCapacityNotSufficient() {
        List<SelectionKey> expectedKeys = new LinkedList<SelectionKey>();
        for (int k = 0; k < FastSelectionKeys.INITIAL_CAPACITY * 4; k++) {
            SelectionKey key = mock(SelectionKey.class);
            expectedKeys.add(key);

            fastSelectionKeys.add(key);
            assertEquals(expectedKeys.size(), fastSelectionKeys.size());
        }

        FastSelectionKeys.SelectionKeys active = fastSelectionKeys.activeKeys;
        assertEquals(active.size, expectedKeys.size());
        for(int k=0;k<expectedKeys.size();k++){
            SelectionKey expected = expectedKeys.get(k);
            SelectionKey found = active.keys[k];
            assertSame(expected, found);
        }
    }

    @Test
    public void add_whenNull(){
       boolean result =  fastSelectionKeys.add(null);

        assertFalse(result);
        FastSelectionKeys.SelectionKeys active = fastSelectionKeys.activeKeys;
        assertEquals(0, active.size);
    }

    // tests the regular nio loop; hasNext, next, remove.
    @Test
    public void testLoop() {
        List<SelectionKey> addedKeys = Arrays.asList(key1, key2, key3);

        for (SelectionKey selectionKey : addedKeys) {
            fastSelectionKeys.add(selectionKey);
        }

        FastSelectionKeys.IteratorImpl it = (FastSelectionKeys.IteratorImpl) fastSelectionKeys.iterator();
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
    public void next_whenNoItem(){
        FastSelectionKeys.IteratorImpl it = (FastSelectionKeys.IteratorImpl) fastSelectionKeys.iterator();

        it.next();
    }

    @Test(expected = IllegalStateException.class)
    public void remove_whenNoItem(){
        FastSelectionKeys.IteratorImpl it = (FastSelectionKeys.IteratorImpl) fastSelectionKeys.iterator();

        it.remove();
    }
}
