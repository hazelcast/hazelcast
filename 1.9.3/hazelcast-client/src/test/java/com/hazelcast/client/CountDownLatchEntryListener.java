/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.client;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;

import java.util.concurrent.CountDownLatch;

public class CountDownLatchEntryListener<K, V> implements EntryListener<K, V> {
    final CountDownLatch entryAddLatch;
    final CountDownLatch entryUpdatedLatch;
    final CountDownLatch entryRemovedLatch;

    public CountDownLatchEntryListener(CountDownLatch entryAddLatch, CountDownLatch entryUpdatedLatch, CountDownLatch entryRemovedLatch) {
        this.entryAddLatch = entryAddLatch;
        this.entryUpdatedLatch = entryUpdatedLatch;
        this.entryRemovedLatch = entryRemovedLatch;
    }

    public void entryAdded(EntryEvent<K, V> event) {
        entryAddLatch.countDown();
//        assertEquals("hello", event.getKey());
    }

    public void entryRemoved(EntryEvent<K, V> event) {
        entryRemovedLatch.countDown();
//        assertEquals("hello", event.getKey());
//        assertEquals("new world", event.getValueData());
    }

    public void entryUpdated(EntryEvent<K, V> event) {
//    	System.out.println(event);
        entryUpdatedLatch.countDown();
//        assertEquals("new world", event.getValueData());
//        assertEquals("hello", event.getKey());
    }

    public void entryEvicted(EntryEvent<K, V> event) {
        entryRemoved(event);
    }
}
