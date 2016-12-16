package com.hazelcast.test.jitter;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.hazelcast.test.jitter.JitterRule.AGGREGATION_INTERVAL_MILLIS;
import static com.hazelcast.test.jitter.JitterRule.CAPACITY;
import static com.hazelcast.util.QuickMath.modPowerOfTwo;

public class JitterRecorder {
    private final AtomicReferenceArray<Slot> slots = new AtomicReferenceArray<Slot>(CAPACITY);

    public void recordPause(long startTimeMillis, long hiccupNanos) {
        Slot slot = getSlotForTimestamp(startTimeMillis);
        slot.recordHiccup(hiccupNanos);
    }

    public Iterable<Slot> getSlotsBetween(long from, long to) {
        long firstBucket = getBucket(from);
        int slotIndex = toSlotIndex(firstBucket);

        ArrayList<Slot> result = new ArrayList<Slot>();

        long minStartTime = firstBucket * AGGREGATION_INTERVAL_MILLIS;
        for (int i = 0; i < CAPACITY; i++) {
            Slot slot = slots.get(slotIndex);
            if (slot != null && slot.getStartIntervalMillis() >= minStartTime && slot.getStartIntervalMillis() <= to) {
                result.add(slot);
            }
            slotIndex = advanceSlotIndex(slotIndex);
        }
        return result;
    }

    private int advanceSlotIndex(int slotIndex) {
        slotIndex++;
        return modPowerOfTwo(slotIndex, CAPACITY);
    }

    private Slot getSlotForTimestamp(long startTime) {
        //bucket on a linear time-line
        long bucket = getBucket(startTime);
        //slot in a circular buffer
        int slotIndex = toSlotIndex(bucket);
        Slot slot = slots.get(slotIndex);
        if (isNullOrStale(slot, bucket)) {
            slot = newSlot(bucket);
            slots.set(slotIndex, slot);
        }
        return slot;
    }

    private int toSlotIndex(long bucket) {
        return (int) modPowerOfTwo(bucket, CAPACITY);
    }

    private boolean isNullOrStale(Slot slot, long bucket) {
        return slot == null || isStaleSlot(slot, bucket);
    }

    private long getBucket(long startTime) {
        return startTime / AGGREGATION_INTERVAL_MILLIS;
    }

    private Slot newSlot(long bucket) {
        return new Slot(bucket * AGGREGATION_INTERVAL_MILLIS);
    }

    private boolean isStaleSlot(Slot slot, long currentBucket) {
        long slotBucket = getBucket(slot.getStartIntervalMillis());
        return slotBucket != currentBucket;
    }

}
