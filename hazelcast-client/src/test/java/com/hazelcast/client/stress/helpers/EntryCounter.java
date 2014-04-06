package com.hazelcast.client.stress.helpers;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;

import java.util.concurrent.atomic.AtomicLong;


public class EntryCounter implements EntryListener {

    public AtomicLong totalAdded= new AtomicLong();
    public AtomicLong totalUpdated= new AtomicLong();

    @Override
    public void entryAdded(EntryEvent event) {
        totalAdded.incrementAndGet();
    }

    @Override
    public void entryRemoved(EntryEvent event) {

    }

    @Override
    public void entryUpdated(EntryEvent event) {
        totalUpdated.incrementAndGet();
    }

    @Override
    public void entryEvicted(EntryEvent event) {

    }
}