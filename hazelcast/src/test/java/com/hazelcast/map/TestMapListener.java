package com.hazelcast.map;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryRemovedListener;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TestMapListener implements EntryAddedListener, EntryRemovedListener, HazelcastInstanceAware {

    public static final AtomicInteger EVENT_COUNT = new AtomicInteger();
    public static final AtomicBoolean INSTANCE_AWARE = new AtomicBoolean();

    private HazelcastInstance instance;

    @Override
    public void entryAdded(EntryEvent event) {
        EVENT_COUNT.incrementAndGet();
    }

    @Override
    public void entryRemoved(EntryEvent event) {
        EVENT_COUNT.incrementAndGet();
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.instance = hazelcastInstance;
        INSTANCE_AWARE.set(this.instance != null);
    }
}