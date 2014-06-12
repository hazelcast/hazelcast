package com.hazelcast.test;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryListener;
import com.hazelcast.map.MapWideEvent;
import com.hazelcast.core.ReplicatedMap;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class WatchedOperationExecutor {

    public void execute(Runnable runnable, int seconds, EntryEventType eventType, ReplicatedMap... replicatedMaps) {
        int[] latches = new int[replicatedMaps.length];
        Arrays.fill(latches, 1);
        execute(runnable, seconds, latches, eventType, replicatedMaps);
    }

    public void execute(Runnable runnable, int seconds, EntryEventType eventType, int operations,
                        ReplicatedMap... replicatedMaps) {

        int[] latches = new int[replicatedMaps.length];
        Arrays.fill(latches, operations);
        execute(runnable, seconds, latches, eventType, replicatedMaps);
    }

    public void execute(Runnable runnable, int seconds, EntryEventType eventType, int operations, double minimalExpectation,
                        ReplicatedMap... replicatedMaps) {

        if (minimalExpectation < 0. || minimalExpectation > 1.) {
            throw new IllegalArgumentException("minimalExpectation range > 0.0, < 1.0");
        }
        int atLeast = (int) (operations * minimalExpectation);
        int[] latches = new int[replicatedMaps.length];
        Arrays.fill(latches, atLeast);
        execute(runnable, seconds, latches, eventType, replicatedMaps);
    }

    public void execute(Runnable runnable, int seconds, int[] latches, EntryEventType eventType,
                        ReplicatedMap... replicatedMaps) {
        final String[] registrationIds = new String[latches.length];
        final WatcherDefinition[] watcherDefinitions = new WatcherDefinition[latches.length];
        for (int i = 0; i < replicatedMaps.length; i++) {
            ReplicatedMap replicatedMap = replicatedMaps[i];
            CountDownLatch latch = new CountDownLatch(latches[i]);
            WatcherListener listener = new WatcherListener(latch, eventType);
            watcherDefinitions[i] = new WatcherDefinition(latch, replicatedMap, listener);
            registrationIds[i] = replicatedMap.addEntryListener(listener);
        }
        try {
            runnable.run();
            long deadline = TimeUnit.SECONDS.toNanos(seconds);
            for (WatcherDefinition definition : watcherDefinitions) {
                long start = System.nanoTime();
                definition.await(deadline, TimeUnit.NANOSECONDS);
                deadline -= System.nanoTime() - start;
                if (deadline <= 0) {
                    return;
                }
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            for (int i = 0; i < replicatedMaps.length; i++) {
                ReplicatedMap replicatedMap = replicatedMaps[i];
                replicatedMap.removeEntryListener(registrationIds[i]);
            }
        }
    }

    private final class WatcherListener
            implements EntryListener {

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

        @Override
        public void evictedAll(MapWideEvent event) {

        }

        private void handleEvent(EntryEventType eventType) {
            if (this.eventType == eventType) {
                latch.countDown();
            }
        }
    }

    private final class WatcherDefinition {
        private final CountDownLatch latch;
        private final ReplicatedMap replicatedMap;
        private final WatcherListener listener;

        private WatcherDefinition(CountDownLatch latch, ReplicatedMap replicatedMap, WatcherListener listener) {
            this.latch = latch;
            this.replicatedMap = replicatedMap;
            this.listener = listener;
        }

        private void await(long timeout, TimeUnit unit)
                throws InterruptedException {

            latch.await(timeout, unit);
        }
    }
}