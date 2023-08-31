package datest;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.listener.EntryUpdatedListener;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public class App {
    public static void main(final String args[]) throws InterruptedException, ExecutionException {
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance();

        final CompletableFuture<Object> entryListenerOldValue = new CompletableFuture<>();
        final CompletableFuture<Object> interceptorOldValue = new CompletableFuture<>();

        final IMap<Object, AtomicInteger> map = instance.getMap("test-map");

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
            return entry.getValue();
        });

        System.out.println("Interceptor put called old: " + interceptorOldValue.get());
        System.out.println("Entry Listener: Value updated from " + entryListenerOldValue.get());

        instance.shutdown();
    }
}
