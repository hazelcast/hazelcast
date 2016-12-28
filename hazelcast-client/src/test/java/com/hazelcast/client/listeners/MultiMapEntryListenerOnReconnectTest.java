package com.hazelcast.client.listeners;

import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.MultiMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MultiMapEntryListenerOnReconnectTest extends AbstractListenersOnReconnectTest {

    private MultiMap multiMap;

    @Override
    protected String addListener() {
        multiMap = client.getMultiMap(randomString());
        final EntryAdapter<Object, Object> listener = new EntryAdapter<Object, Object>() {
            public void onEntryEvent(EntryEvent<Object, Object> event) {
                eventCount.incrementAndGet();
            }
        };
        return multiMap.addEntryListener(listener, true);
    }

    @Override
    public void produceEvent() {
        multiMap.put(randomString(), randomString());
    }

    @Override
    public boolean removeListener(String registrationId) {
        return multiMap.removeEntryListener(registrationId);
    }
}