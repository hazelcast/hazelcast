package com.hazelcast.map.impl;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.MapEvent;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class EntryListenerAdaptorsTest {

    /**
     * 'EntryListenerAdaptors.createListenerAdapters' method should create all appropriate listener-adapters
     * for the extended interfaces of EntryListener interface, which are:
     * - EntryAddedListener,
     * - EntryUpdatedListener,
     * - EntryRemovedListener,
     * - EntryEvictedListener,
     * - MapClearedListener,
     * - MapEvictedListener
     *
     * @see EntryListener
     */
    @Test
    public void test_createListenerAdapters() {
        TestEntryListener listener = new TestEntryListener();
        ListenerAdapter[] listenerAdapters = EntryListenerAdaptors.createListenerAdapters(listener);
        for (ListenerAdapter<?> listenerAdapter : listenerAdapters) {
            // just pass null to trigger corresponding listener method calls.
            listenerAdapter.onEvent(null);
        }

        String msg = "should be called exactly 1 times";
        assertEquals(msg, listener.entryAddedCalled, 1);
        assertEquals(msg, listener.entryEvictedCalled, 1);
        assertEquals(msg, listener.entryRemovedCalled, 1);
        assertEquals(msg, listener.entryUpdatedCalled, 1);
        assertEquals(msg, listener.mapClearedCalled, 1);
        assertEquals(msg, listener.mapEvictedCalled, 1);
    }

    private class TestEntryListener implements EntryListener {

        int entryAddedCalled;
        int entryEvictedCalled;
        int entryRemovedCalled;
        int entryUpdatedCalled;
        int mapClearedCalled;
        int mapEvictedCalled;

        @Override
        public void entryAdded(EntryEvent event) {
            entryAddedCalled++;
        }

        @Override
        public void entryEvicted(EntryEvent event) {
            entryEvictedCalled++;
        }

        @Override
        public void entryRemoved(EntryEvent event) {
            entryRemovedCalled++;
        }

        @Override
        public void entryUpdated(EntryEvent event) {
            entryUpdatedCalled++;
        }

        @Override
        public void mapCleared(MapEvent event) {
            mapClearedCalled++;
        }

        @Override
        public void mapEvicted(MapEvent event) {
            mapEvictedCalled++;
        }
    }
}
