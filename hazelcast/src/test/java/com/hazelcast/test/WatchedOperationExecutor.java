package com.hazelcast.test;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.MapEvent;
import com.hazelcast.core.ReplicatedMap;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class WatchedOperationExecutor {

    public void execute(Runnable runnable, int seconds, EntryEventType eventType, ReplicatedMap... replicatedMaps)
            throws TimeoutException {
        int[] latches = new int[replicatedMaps.length];
        Arrays.fill(latches, 1);
        execute(runnable, seconds, latches, eventType, replicatedMaps);
    }

    public void execute(Runnable runnable, int seconds, EntryEventType eventType, int operations, ReplicatedMap... replicatedMaps)
            throws TimeoutException {
        int[] latches = new int[replicatedMaps.length];
        Arrays.fill(latches, operations);
        execute(runnable, seconds, latches, eventType, replicatedMaps);
    }

    public void execute(Runnable runnable, int timeoutSeconds, EntryEventType eventType, int operations,
                        double minimalExpectation, ReplicatedMap... replicatedMaps) throws TimeoutException {
        if (minimalExpectation < 0.0 || minimalExpectation > 1.0) {
            throw new IllegalArgumentException("minimalExpectation range > 0.0, < 1.0");
        }
        int atLeast = (int) (operations * minimalExpectation);
        int[] latches = new int[replicatedMaps.length];
        Arrays.fill(latches, atLeast);
        execute(runnable, timeoutSeconds, latches, eventType, replicatedMaps);
    }

    public <K, V> void execute(Runnable runnable, int timeoutSeconds, int[] latches, EntryEventType eventType,
                               ReplicatedMap<K, V>... replicatedMaps) throws TimeoutException {
        String[] registrationIds = new String[latches.length];
        WatcherDefinition[] watcherDefinitions = new WatcherDefinition[latches.length];
        for (int i = 0; i < replicatedMaps.length; i++) {
            ReplicatedMap<K, V> replicatedMap = replicatedMaps[i];
            CountDownLatch latch = new CountDownLatch(latches[i]);
            WatcherListener<K, V> listener = new WatcherListener<K, V>(latch, eventType);
            watcherDefinitions[i] = new WatcherDefinition(latch);
            registrationIds[i] = replicatedMap.addEntryListener(listener);
        }
        try {
            runnable.run();
            long deadline = TimeUnit.SECONDS.toNanos(timeoutSeconds);
            for (WatcherDefinition definition : watcherDefinitions) {
                long start = System.nanoTime();
                definition.await(deadline, TimeUnit.NANOSECONDS);
                deadline -= System.nanoTime() - start;
                if (deadline <= 0) {
                    throw new TimeoutException("Deadline reached. Remaining: " + definition.latch.getCount());
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

    private final class WatcherListener<K, V> implements EntryListener<K, V> {

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
        public void mapEvicted(MapEvent event) {
        }

        @Override
        public void mapCleared(MapEvent event) {
        }

        private void handleEvent(EntryEventType eventType) {
            if (this.eventType == eventType) {
                latch.countDown();
            }
        }

        @Override
        public String toString() {
            return "WatcherListener{" +
                    "latch=" + latch +
                    ", eventType=" + eventType +
                    '}';
        }
    }

    private final class WatcherDefinition {

        private final CountDownLatch latch;

        private WatcherDefinition(CountDownLatch latch) {
            this.latch = latch;
        }

        private void await(long timeout, TimeUnit unit) throws InterruptedException {
            latch.await(timeout, unit);
        }

        @Override
        public String toString() {
            return "WatcherDefinition{" +
                    "latch=" + latch +
                    '}';
        }
    }
}
