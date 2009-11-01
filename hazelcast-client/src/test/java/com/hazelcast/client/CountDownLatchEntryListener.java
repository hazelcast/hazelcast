package com.hazelcast.client;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.CountDownLatch;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;

public class CountDownLatchEntryListener<K, V> implements EntryListener<K, V>{
	final CountDownLatch entryAddLatch;
    final CountDownLatch entryUpdatedLatch;
    final CountDownLatch entryRemovedLatch;
	public CountDownLatchEntryListener(CountDownLatch entryAddLatch, CountDownLatch entryUpdatedLatch, CountDownLatch entryRemovedLatch) {
		this.entryAddLatch = entryAddLatch;
		this.entryUpdatedLatch = entryUpdatedLatch;
		this.entryRemovedLatch = entryRemovedLatch;
	}

	public void entryAdded(EntryEvent<K, V> event) {
//		System.out.println(event);
    	entryAddLatch.countDown();
        assertEquals("hello", event.getKey());
    }

    public void entryRemoved(EntryEvent<K, V> event) {
//    	System.out.println(event);
    	entryRemovedLatch.countDown();
        assertEquals("hello", event.getKey());
        assertEquals("new world", event.getValue());
    }

    public void entryUpdated(EntryEvent<K, V> event) {
//    	System.out.println(event);
    	entryUpdatedLatch.countDown();
        assertEquals("new world", event.getValue());
        assertEquals("hello", event.getKey());
    }

    public void entryEvicted(EntryEvent<K, V> event) {
        entryRemoved(event);
    }
	
}