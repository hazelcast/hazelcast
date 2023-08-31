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
import com.hazelcast.test.annotation.SlowTest;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(HazelcastParallelClassRunner.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class EntryUpdatedListenerTest extends HazelcastTestSupport {
    /**
     * <a href="https://hazelcast.atlassian.net/browse/HZ-2837">HZ-2837 - Field level mutation being taken by listener as old
     * value but not being considered by interceptor - Strange Behavior</a>
     */
    @Test
    public void testOldValues() throws InterruptedException, ExecutionException {
        final HazelcastInstance instance = createHazelcastInstanceFactory().newHazelcastInstance();

        final CompletableFuture<Object> entryListenerOldValue = new CompletableFuture<>();
        final CompletableFuture<Object> interceptorOldValue = new CompletableFuture<>();

        final IMap<Object, AtomicInteger> map = instance.getMap(randomMapName());

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
                    interceptorOldValue.complete(oldValue);
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

        map.addLocalEntryListener(
                (EntryUpdatedListener<Object, AtomicInteger>) event -> entryListenerOldValue.complete(event.getOldValue()));

        final Object key = Void.TYPE;

        map.set(key, new AtomicInteger(1));

        map.executeOnKey(key, entry -> {
            entry.getValue().set(Integer.MAX_VALUE);
            entry.setValue(new AtomicInteger(2));
            return null;
        });

        assertEqualsStringFormat(
                "Old values observed by MapInterceptor.interceptPut (%s) & EntryUpdatedListener.entryUpdated (%s) differ",
                interceptorOldValue.get(), entryListenerOldValue.get());
    }
}
