package datest;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class App {
    private static class TestInterceptor implements MapInterceptor {
        private static final long serialVersionUID = 4971835785800511499L;

        @Override
        public Object interceptGet(final Object value) {
            return value;
        }

        @Override
        public void afterGet(final Object value) {
        }

        @Override
        public Object interceptPut(final Object oldValue, final Object newValue) {
            System.out.println("Interceptor put called old: " + oldValue + " new: " + newValue);
            if (newValue.equals(new AtomicReference<>("Version2"))) {
                System.out.println(
                        "Interceptor put called old: " + oldValue + ", new: " + newValue + ", replaced with 'Version3'");
                return new AtomicReference<>("Version3");
            }
            return newValue != null ? newValue : oldValue;
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
    }

    private static class TestListener implements EntryAddedListener<String, AtomicReference<String>>,
            EntryUpdatedListener<String, AtomicReference<String>> {
        @Override
        public void entryAdded(final EntryEvent<String, AtomicReference<String>> event) {
            System.out.println("Entry Listener: Value added: " + event.getValue());
        }

        @Override
        public void entryUpdated(final EntryEvent<String, AtomicReference<String>> event) {
            System.out.println("Entry Listener: Value updated from " + event.getOldValue() + " to " + event.getValue());
        }
    }

    private static class TestProcessor implements EntryProcessor<String, AtomicReference<String>, AtomicReference<String>> {
        private static final long serialVersionUID = -7228575928294057876L;
        private final AtomicReference<String> newValue;

        TestProcessor(final AtomicReference<String> newValue) {
            this.newValue = newValue;
        }

        @Override
        public AtomicReference<String> process(final Map.Entry<String, AtomicReference<String>> entry) {
            System.out
                    .println("Entry processor called to update new value: " + entry.getValue() + " with new value " + newValue);
            entry.getValue().set("ignoreIt");
            entry.setValue(newValue);
            return newValue;
        }
    }

    public static void main(final String args[]) throws InterruptedException {
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance();

        final IMap<String, AtomicReference<String>> map = instance.getMap("test-map");
        map.addInterceptor(new TestInterceptor());
        map.addLocalEntryListener(new TestListener());

        final AtomicReference<String> ref = new AtomicReference<>("Version1");
        System.out.println("Adding new entry -> " + ref);
        map.put("key", ref);

        TimeUnit.SECONDS.sleep(1);

        System.out.println(
                "Performing executeOnKey - update existing entry using entry processor value1->value2, expect interceptor to change field1 from value2 to value3");
        map.executeOnKey("key", new TestProcessor(new AtomicReference<>("Version2")));
    }
}
