package datest;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;

public class TestListener implements EntryAddedListener<String, TestPojo>, EntryUpdatedListener<String, TestPojo>,
        EntryRemovedListener<String, TestPojo> {
    @Override
    public void entryAdded(final EntryEvent<String, TestPojo> event) {
        System.out.println("Entry Listener: Value added: " + event.getValue());
    }

    @Override
    public void entryRemoved(final EntryEvent<String, TestPojo> event) {
    }

    @Override
    public void entryUpdated(final EntryEvent<String, TestPojo> event) {
        System.out.println("Entry Listener: Value updated from " + event.getOldValue() + " to " + event.getValue());
    }
}
