package com.hazelcast.test;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.ReplicatedMap;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class WatchedOperationExecutor {

    public void execute(Runnable runnable, int seconds,
                        EntryEventType eventType, ReplicatedMap... replicatedMaps) {
        execute(runnable, seconds, replicatedMaps.length, eventType, replicatedMaps);
    }

    public void execute(Runnable runnable, int seconds, int latches,
                        EntryEventType eventType, ReplicatedMap... replicatedMaps) {
        final String[] registrationIds = new String[latches];
        final CountDownLatch latch = new CountDownLatch(latches);
        final WatcherListener listener = new WatcherListener(latch, eventType);
        for (int i = 0; i < replicatedMaps.length; i++) {
            ReplicatedMap replicatedMap = replicatedMaps[i];
            registrationIds[i] = replicatedMap.addEntryListener(listener);
        }
        try {
            runnable.run();
            latch.await(seconds, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            for (int i = 0; i < replicatedMaps.length; i++) {
                ReplicatedMap replicatedMap = replicatedMaps[i];
                replicatedMap.removeEntryListener(registrationIds[i]);
            }
        }
    }

    private class WatcherListener implements EntryListener {
        private final CountDownLatch latch;
        private final EntryEventType eventType;

        private WatcherListener(CountDownLatch latch, EntryEventType eventType) {
            this.latch = latch;
            this.eventType = eventType;
        }

        @Override
        public void entryAdded(EntryEvent event) {
            handleEvent(event.getEventType());
        }

        @Override
        public void entryRemoved(EntryEvent event) {
            handleEvent(event.getEventType());
        }

        @Override
        public void entryUpdated(EntryEvent event) {
            handleEvent(event.getEventType());
        }

        @Override
        public void entryEvicted(EntryEvent event) {
            handleEvent(event.getEventType());
        }

        private void handleEvent(EntryEventType eventType) {
            if (this.eventType == eventType) {
                latch.countDown();
            }
        }
    }

}