package com.hazelcast.spi.impl.operationexecutor.impl;

import com.hazelcast.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.util.concurrent.IdleStrategy;

import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.hazelcast.nio.Bits.CACHE_LINE_LENGTH;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * The locks for the partitions.
 *
 * The locks are stored in an array where padding is applied to prevent false sharing.
 */
final class PartitionLocks {
    private static final long IDLE_MAX_SPINS = 20;
    private static final long IDLE_MAX_YIELDS = 50;
    private static final long IDLE_MIN_PARK_NS = NANOSECONDS.toNanos(1);
    private static final long IDLE_MAX_PARK_NS = MICROSECONDS.toNanos(100);

    // Strategy used when the partition is locked.
    private static final IdleStrategy IDLE_STRATEGY
            = new BackoffIdleStrategy(IDLE_MAX_SPINS, IDLE_MAX_YIELDS, IDLE_MIN_PARK_NS, IDLE_MAX_PARK_NS);

    private final AtomicReferenceArray<Thread> locks;

    PartitionLocks(int partitionCount) {
        //todo: first item could end up with false sharing.
        this.locks = new AtomicReferenceArray<Thread>(partitionCount * CACHE_LINE_LENGTH);
    }

    public void unlock(int partitionId) {
        locks.set(toIndex(partitionId), null);
    }

    private static int toIndex(int partitionId) {
        return partitionId * CACHE_LINE_LENGTH;
    }

    /**
     * Tries to acquire the lock for the given partition.
     *
     * There is no support for reentrant lock acquisition.
     *
     * @param partitionId the id of the partition
     * @param owner       the
     * @return true if the lock was acquired, false otherwise.
     */
    public boolean tryLock(int partitionId, Thread owner) {
        //assert owner != null : "owner can't be null";
        return locks.compareAndSet(toIndex(partitionId), null, owner);
    }

    /**
     * Returns the Thread that currently owns the lock, or null if the lock is free.
     *
     * @param partitionId the id of the partition
     * @return the current lock owner.
     */
    public Thread getOwner(int partitionId) {
        return locks.get(toIndex(partitionId));
    }

    public void lock(int partitionId, Thread owner) {
        long iteration = 0;
        long startMs = System.currentTimeMillis();
        for (; ; ) {
            if (getOwner(partitionId) == owner) {
                throw new RuntimeException("Reentrant lock acquire detected by: " + owner);//todo: better exception
            } else if (tryLock(partitionId, owner)) {
                return;
            }

            iteration++;
            IDLE_STRATEGY.idle(iteration);

            // hack
            if (System.currentTimeMillis() > startMs + SECONDS.toMillis(30)) {
                throw new RuntimeException("Waiting too long for lock");
            }
        }
    }
}
