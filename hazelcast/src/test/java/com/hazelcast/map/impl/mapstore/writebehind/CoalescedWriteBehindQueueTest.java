package com.hazelcast.map.impl.mapstore.writebehind;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.map.impl.mapstore.writebehind.entry.DelayedEntry;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;

import static com.hazelcast.map.impl.mapstore.writebehind.entry.DelayedEntries.createWithoutValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CoalescedWriteBehindQueueTest extends HazelcastTestSupport {

    private SerializationService serializationService = new DefaultSerializationServiceBuilder().build();
    private CoalescedWriteBehindQueue queue = new CoalescedWriteBehindQueue();


    @Test
    public void test_addFirst() throws Exception {
        queue.addFirst(Collections.<DelayedEntry>singletonList(newEntry(1)));

        assertEquals(1, queue.size());
    }

    @Test
    public void test_addLast() throws Exception {
        queue.addLast(newEntry(1));

        assertEquals(1, queue.size());
    }

    @Test
    public void test_removeFirstOccurrence() throws Exception {
        DelayedEntry<Data, Object> entry = newEntry(1);
        queue.addLast(entry);
        queue.removeFirstOccurrence(entry);

        assertEquals(0, queue.size());
    }

    @Test
    public void test_contains() throws Exception {
        DelayedEntry<Data, Object> entry = newEntry(1);
        queue.addLast(entry);

        assertTrue(queue.contains(entry));
    }

    @Test
    public void test_size() throws Exception {
        DelayedEntry<Data, Object> entry = newEntry(1);
        queue.addLast(entry);
        queue.addLast(entry);
        queue.addLast(entry);

        assertEquals(1, queue.size());
    }

    @Test
    public void test_clear() throws Exception {
        DelayedEntry<Data, Object> entry = newEntry(1);
        queue.addLast(entry);

        queue.clear();
        assertEquals(0, queue.size());
    }

    protected DelayedEntry<Data, Object> newEntry(Object key) {
        return createWithoutValue(serializationService.toData(key));
    }
}