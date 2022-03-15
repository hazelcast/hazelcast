/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.map.impl.mapstore.writebehind;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.map.impl.mapstore.MapStoreContext;
import com.hazelcast.map.impl.mapstore.writebehind.entry.DelayedEntries;
import com.hazelcast.map.impl.mapstore.writebehind.entry.DelayedEntry;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.starter.ReflectionUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WriteBehindStoreTest {

    private MapStoreContext mapStoreContext = mock(MapStoreContext.class, RETURNS_DEEP_STUBS);
    private WriteBehindProcessor writeBehindProcessor = mock(WriteBehindProcessor.class, RETURNS_DEEP_STUBS);

    @Test
    public void shouldNotChangeSequenceAfterAddingToQueue() throws IllegalAccessException {
        // given
        when(mapStoreContext.getMapServiceContext().getNodeEngine().getSerializationService()).thenReturn(mock(InternalSerializationService.class));
        WriteBehindStore store = new WriteBehindStore(mapStoreContext, 1, writeBehindProcessor);
        DummyQueue queue = mock(DummyQueue.class, CALLS_REAL_METHODS);
        ReflectionUtils.setFieldValueReflectively(store, "writeBehindQueue", queue);
        DelayedEntry<Data, Object> delayedEntry = DelayedEntries.newAddedDelayedEntry(mock(HeapData.class), new Entry(1, 1), 0, 0, 1, null);
        assertEquals(delayedEntry.getSequence(), 0L);

        // when
        store.add(delayedEntry);

        // then
        assertEquals(delayedEntry.getSequence(), 1L);
        assertEquals(queue.getSequence(), 1L);

    }

    private static final class Entry implements Serializable {

        private int id;
        private int version;

        Entry() {
            // serialization
        }

        Entry(int id, int version) {
            this.id = id;
            this.version = version;
        }

        Entry newVersion() {
            return new Entry(id, version + 1);
        }

    }

    abstract static class DummyQueue implements WriteBehindQueue {

        long sequence = 0;

        public long getSequence() {
            return sequence;
        }

        @Override
        public void addLast(Object o, boolean addWithoutCapacityCheck) {
            if (o instanceof DelayedEntry) {
                sequence = ((DelayedEntry<?, ?>) o).getSequence();
            }
        }


    }
}
