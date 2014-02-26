package com.hazelcast.client.stress.helpers;

import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;

import java.util.concurrent.atomic.AtomicLong;

public class ItemCounter implements ItemListener {

    public AtomicLong runningCount= new AtomicLong();
    public AtomicLong totalAdded = new AtomicLong();
    public AtomicLong totalRemoved= new AtomicLong();

    @Override
    public void itemAdded(ItemEvent item) {
        totalAdded.incrementAndGet();
        runningCount.incrementAndGet();
    }

    @Override
    public void itemRemoved(ItemEvent item) {
        totalRemoved.incrementAndGet();
        runningCount.decrementAndGet();
    }

}


