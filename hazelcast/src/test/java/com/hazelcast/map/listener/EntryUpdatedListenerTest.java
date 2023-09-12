package com.hazelcast.map.listener;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EntryUpdatedListenerTest extends HazelcastTestSupport {
    /**
     * @see <a href="https://hazelcast.atlassian.net/browse/HZ-2837">HZ-2837 - Field level mutation being taken by listener as
     *      old value but not being considered by interceptor - Strange Behaviour</a>
     */
    @Test
    public void testOldValues() throws InterruptedException, ExecutionException {
        final HazelcastInstance instance = createHazelcastInstanceFactory().newHazelcastInstance();

        // Create a map
        final IMap<Object, AtomicInteger> map = instance.getMap(randomMapName());

        // Setup the listeners
        final CompletableFuture<Integer> entryListenerOldValue = setMapListener(
                listener -> map.addEntryListener(listener, true));
        final CompletableFuture<Integer> entryLocalListenerOldValue = setMapListener(map::addLocalEntryListener);

        final CompletableFuture<Integer> interceptorOldValue = new CompletableFuture<>();
        map.addInterceptor(new MapInterceptor() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object interceptGet(final Object value) {
                return value;
            }

            @Override
            public void afterGet(final Object value) {
            }

            @Override
            public Object interceptPut(final Object oldValue, final Object newValue) {
                if (oldValue != null) {
                    interceptorOldValue.complete(((AtomicInteger) oldValue).get());
                }

                return null;
            }

            @Override
            public void afterPut(final Object value) {
            }

            @Override
            public Object interceptRemove(final Object removedValue) {
                return null;
            }

            @Override
            public void afterRemove(final Object oldValue) {
            }
        });

        // Insert a dummy initial value
        final Object key = Void.TYPE;
        final int initial = 1;

        map.set(key, new AtomicInteger(initial));

        map.executeOnKey(key, entry -> {
            // Mutate the value in the map directly - expectation is that this will not be visible to anyone -
            // https://hazelcast.atlassian.net/browse/HZ-2837?focusedCommentId=82402
            entry.getValue().set(Integer.MAX_VALUE);

            // Also, update the map entry with a new value
            entry.setValue(new AtomicInteger(2));

            return null;
        });

        // Check all the listeners received the correct response
        assertEqualsStringFormat(
                "Initial value provided (%s) does not match old value observed by EntryUpdatedListener.entryUpdated (%s) differ",
                initial, entryListenerOldValue.get());

        assertEqualsStringFormat(
                "Initial value provided (%s) does not match old value observed by Local EntryUpdatedListener.entryUpdated (%s) differ",
                initial, entryLocalListenerOldValue.get());

        assertEqualsStringFormat(
                "Initial value provided (%s) does not match old value observed by MapInterceptor.interceptPut (%s)", initial,
                interceptorOldValue.get());
    }

    /**
     * Constructs a {@link EntryUpdatedListener}, registering it via {@code listenerSetter}
     *
     * @return a {@link CompleteableFuture} referencing the {@link EntryEvent.getOldValue()} from when the
     *         {@link EntryUpdatedListener} was fired
     */
    private static CompletableFuture<Integer> setMapListener(
            final Consumer<EntryUpdatedListener<Object, AtomicInteger>> listenerSetter) {
        final CompletableFuture<Integer> oldValue = new CompletableFuture<>();
        listenerSetter
                .accept((EntryUpdatedListener<Object, AtomicInteger>) event -> oldValue.complete(event.getOldValue().get()));
        return oldValue;
    }
}
