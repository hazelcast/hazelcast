package com.hazelcast.spring;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;

public class DummyEntryListener implements EntryListener {
	
	public void entryAdded(EntryEvent event) {
		System.err.println("Added: " + event);
	}

	public void entryRemoved(EntryEvent event) {
		System.err.println("Removed: " + event);
	}

	public void entryUpdated(EntryEvent event) {
		System.err.println("Updated: " + event);
	}

	public void entryEvicted(EntryEvent event) {
		System.err.println("Evicted: " + event);
	}
}
