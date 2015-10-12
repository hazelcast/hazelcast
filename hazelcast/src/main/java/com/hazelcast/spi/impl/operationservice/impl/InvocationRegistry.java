/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.HazelcastOverloadException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.internal.metrics.MetricsProvider;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.util.ThreadLocalRandom;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.util.QuickMath;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.internal.util.counters.MwCounter.newMwCounter;
import static com.hazelcast.nio.Bits.CACHE_LINE_LENGTH;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.spi.OperationAccessor.setCallId;
import static com.hazelcast.spi.OperationAccessor.setCallTimeout;
import static com.hazelcast.spi.properties.GroupProperty.BACKPRESSURE_BACKOFF_TIMEOUT_MILLIS;
import static com.hazelcast.spi.properties.GroupProperty.MAX_CONCURRENT_INVOCATIONS;
import static com.hazelcast.spi.properties.GroupProperty.PARTITION_COUNT;
import static com.hazelcast.util.Preconditions.checkNotNegative;
import static com.hazelcast.util.Preconditions.checkPositive;
import static java.lang.Math.min;
import static java.lang.Math.pow;
import static java.lang.Math.round;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.locks.LockSupport.parkNanos;

/**
 * The InvocationsRegistry is responsible for the registration of all pending invocations. Using the InvocationRegistry the
 * Invocation and its response(s) can be linked to each other. When an invocation is registered, a callId is generated. Based
 * on this callId, the invocation and its response can be linked to each other.
 *
 * The InvocationRegistry differentiates between:
 * <ol>
 * <li>low priority invocations like a map.put</li>
 * <li>high priority invocations like cluster/system operations</li>
 * </ol>
 * The low priority invocations are stored isolated from high priority invocations. For low priority invocations no litter is
 * being created and to prevent overload, there is a bound on the number of concurrent low priority invocations. For high
 * priority invocations no back pressure is applied because they should not blocked under any circumstance. Also they are quite
 * rare, so we don't care if litter is created or not.
 *
 * <h2>Low Priority Invocations</h2>
 * Low priority invocations have relatively uncontended callId sequence and the invocations are stored in the
 * {@link #lowInvocations} AtomicReferenceArray to prevent {@link Long} and CHM.Node litter. Most of the calls in HZ are
 * low priority operations.
 *
 * <h3>Partition specific and member specific invocations</h3>
 * There are 2 types of invocations:
 * <ol>
 * <li>partition specific e.g. an invocation for partition 25</li>
 * <li>generic (member specific) e.g. for member 192.168.1.102:5701</li>
 * </ol>
 * In case of a low priority member specific invocation, a random partition-id is selected by using a ThreadLocalRandom in
 * combination with the partitionCount. Using this approach the registration of such an invocation is simplified since is behaves
 * like a partition specific invocation.
 *
 * However it could happen that if a stripe of invocation is filled and a partition-id for that strip is selected at random, that
 * the target specific invocation can't be placed, even though another stripe might have space available.
 *
 * todo:
 * One stripe full, while other stripes empty.
 *
 * <h3>Low priority call id's</h3>
 * In Hazelcast 3.6 and before a single AtomicLong was used as a sequence to generate call id's for all invocations. The
 * problem is that this causes contention since all threads will fight for the same cache line. To reduce contention for
 * low priority invocations, there is a {@link AtomicLongArray} called the {@link #lowSequences} of longs. In this array
 * the sequences are padded to prevent false sharing.
 *
 * Each sequence in the array is initialized with the sequenceIndex. If concurrency level is 10, the following sequences are
 * generated:
 * <ol>
 * <li>sequence 0 generates 1, 11, 21, 31, ...</li>
 * <li>sequence 1 generates 2, 12, 22, 32, ...</li>
 * <li>...</li>
 * <li>sequence 9 generates 10, 20, 30, 40, ...</li>
 * </ol>
 * Low priority call id's are always equal or larger than 1.
 *
 * The low priority call id's can wrap; in that case they will be reset to their initial value. So the same id could be generated
 * twice, but it would take a very long time for it to happen. With a concurrency level of 512 and 10^9 invocations per
 * second, it would take +/- 6 months for this to happen. When a duplicate id is created and a pending invocation is found in
 * the lowInvocations array, a new id is generated. So it will not happen that 2 invocations, invoked by the same member,
 * with the same sequence id are active at any given moment.
 *
 * <h3>Back pressure</h3>
 *
 * todo:
 * Because low priority invocations are stored in an array, there is a maximum number of invocations that can be pending. This is
 * needed to prevent overloading the system with pending requests which could lead to an OOME.
 *
 * <h2>High priority Invocations</h2>
 * High priority invocations work similar as in Hazelcast 3.6 and older. The high priority invocations are stored in a
 * ConcurrentHashMap and there is no bound on the number of pending high priority invocations. Also no back pressure is applied
 * since we don't want to block system services sending such invocations.
 *
 * High priority invocations generate additional litter compared to low priority ones;
 * <ol>
 * <li>the Node in the {@link ConcurrentHashMap}</li>
 * <li>the Long representing the callId is created multiple times. For a simple Map.get it is created 3 times; one time for
 * registration, one for {@link #get(long)} and one time for {@link #deregister(Invocation)}</li>
 * </ol>
 *
 * High priority invocations get a negative callId which keeps decrementing by 1, unlike low priority operations, which get a
 * positive callId and are incremented with <code>concurrencyLevel</code> There is some contention on the {@link #highSequence}
 * since it is shared between all threads, but high priority operations are not that frequent so contention should not be an
 * issue. No wrap detection is added to the highSequence because with 10^9 invocations per second, it would take almost 300 year
 * for the counter to wrap.
 *
 * High priority invocations are completely independent of low priority invocations. Also high priority operations do not claim
 * space in the low priority invocations array.
 *
 * Based on the partitionId the right stripe in the array is selected. If the call is not a partition specific call, then a random
 * partitionId is selected so that these calls are equally spread.
 *
 * Todo:
 * - back pressure time calculation doesn't work
 */
@SuppressWarnings("checkstyle:magicnumber")
public class InvocationRegistry implements Iterable<Invocation>, MetricsProvider {

    // To prevent false sharing between callId-generators sequences, each long sequence is on a separate cache line.
    // The alignment is expressed in longs; not in bytes. So on a system with a cacheline size of 64 bytes
    // the sequence alignment is 8 longs; so in total there are 7 padded longs trailing after each sequence.
    static final int SEQUENCE_ALIGNMENT = CACHE_LINE_LENGTH / LONG_SIZE_IN_BYTES;

    private static final int MAX_DELAY_MS = 500;

    // Contains the low priority sequences.
    // this array will have length 'concurrencyLevel * SEQUENCE_ALIGNMENT' to prevent false sharing.
    final AtomicLongArray lowSequences;

    // the length of the invocation array (will be a positive power of two)
    final int lowInvocationsLength;

    // high priority sequences are negative and start from -1.
    final AtomicLong highSequence = new AtomicLong(-1);

    // contains the high priority invocations
    @Probe(name = "invocations.pending[highPriority]", level = MANDATORY)
    final ConcurrentMap<Long, Invocation> highInvocations = new ConcurrentHashMap<Long, Invocation>();

    // contains the low priority invocations
    final AtomicReferenceArray<Invocation> lowInvocations;

    @Probe
    private final Counter backoffTotalMs = newMwCounter();

    private final int partitionCount;
    private final int concurrencyLevel;
    private final int backoffTimeoutMs;
    private final ILogger logger;

    public InvocationRegistry(HazelcastProperties properties, ILogger logger, int concurrencyLevel) {
        this.logger = logger;
        // we upgrade the concurrency to power of 2 so we can do a cheap mod.
        this.concurrencyLevel = QuickMath.nextPowerOfTwo(concurrencyLevel);

        this.backoffTimeoutMs = getBackoffTimeoutMs(properties);
        this.partitionCount = properties.getInteger(PARTITION_COUNT);

        // todo: can the length cause problems due to wrapping and ending in another stripe?
        this.lowInvocationsLength = getMaxConcurrentInvocations(properties);

        //todo: do we want to deal with padding for false sharing here?
        // the problem is that is will consume more memory than without padding.
        // and how much chance is there that 2 threads at the same time will hit the same cache line
        // since subsequent calls for the same partition will be very far apart.
        this.lowInvocations = new AtomicReferenceArray<Invocation>(lowInvocationsLength);

        this.lowSequences = newLowSequences();
    }

    @Override
    public void provideMetrics(MetricsRegistry metricsRegistry) {
        metricsRegistry.scanAndRegister(this, "operation");
    }

    private static int getMaxConcurrentInvocations(HazelcastProperties props) {
        int invocations = props.getInteger(MAX_CONCURRENT_INVOCATIONS);

        checkPositive(invocations, "Can't have '" + MAX_CONCURRENT_INVOCATIONS + "' with a value smaller than 1");

        return QuickMath.nextPowerOfTwo(invocations);
    }

    private static int getBackoffTimeoutMs(HazelcastProperties props) {
        int backoffTimeoutMs = (int) props.getMillis(BACKPRESSURE_BACKOFF_TIMEOUT_MILLIS);

        return checkNotNegative(backoffTimeoutMs, "Can't have '" + BACKPRESSURE_BACKOFF_TIMEOUT_MILLIS
                + "' with a value smaller than 0");
    }

    /**
     * Initiates the low priority sequences.
     *
     * @return the created AtomicLongArray.
     */
    private AtomicLongArray newLowSequences() {
        AtomicLongArray lowSequences = new AtomicLongArray(concurrencyLevel * SEQUENCE_ALIGNMENT);

        for (int sequenceIndex = 0; sequenceIndex < concurrencyLevel; sequenceIndex++) {
            lowSequences.set(sequenceIndex * SEQUENCE_ALIGNMENT, sequenceIndex);
        }

        return lowSequences;
    }

    // ======================== get ==================================

    /**
     * Gets the invocation for the given call id.
     *
     * @param callId the callId.
     * @return the Invocation for the given callId, or null if no invocation was found.
     */
    public Invocation get(long callId) {
        if (callId < 0) {
            return getHigh(callId);
        } else {
            return getLow(callId);
        }
    }

    private Invocation getHigh(long callId) {
        return highInvocations.get(callId);
    }

    private Invocation getLow(long callId) {
        Invocation invocation = lowInvocations.get(lowInvocationIndex(callId));

        if (invocation == null || invocation.op.getCallId() != callId) {
            // no invocation was found, or an invocation with a different callId was found in the same slot.
            // in both cases we return null.
            return null;
        }

        return invocation;
    }

    // ======================== register ==================================

    /**
     * Registers an invocation.
     *
     * @param invocation The invocation to register.
     */
    public void register(Invocation invocation) {
        assert invocation.op.getCallId() == 0 : "can't register twice: " + invocation;

        if (invocation.op.isUrgent()) {
            registerHigh(invocation);
        } else {
            registerLow(invocation);
        }
    }

    /**
     * Registers a high priority invocation.
     *
     * @param invocation the invocation to register.
     */
    private void registerHigh(Invocation invocation) {
        // we use negative callId's to indicate high priority invocations.
        long callId = highSequence.getAndDecrement();

        setCallId(invocation.op, callId);

        // registers the invocation in the highInvocations-CHM
        // at this point other threads could access the invocation.
        highInvocations.put(callId, invocation);
    }

    /**
     * Registers a low priority invocation.
     *
     * Low priority invocation registration is a bit more complex. First we determine the partition-id (for a invocation
     * that isn't partition specific, we randomly select a partition-id) and based on this partitionId we determine and
     * idGenerator.
     *
     * The next step is determining a callId. We increment the idGenerator with the idGeneratorsCount and based on this
     * callId we determine the slot in the lowInvocations-AtomicReferenceArray. If slot is free and we can cas
     * in the invocation, we are done. But if the slot it taken, we need to generate a new callId.
     *
     * Eventually the following things can happen:
     * <ol>
     * <ol>a slot is found</ol>
     * <ol>the system repeatedly no slot is found repeatedly. In this case the back-pressure should kick in.</ol>
     * </ol>
     *
     * @param invocation the invocation to register.
     */
    private void registerLow(Invocation invocation) {
        int partitionId = partitionId(invocation);

        int sequenceIndex = lowSequenceIndex(partitionId);
        int iteration = 0;
        long remainingTimeoutMs = 0;
        for (; ; ) {
            // First we determine a call-id and set it in the operation.
            long callId = newLowCallId(sequenceIndex);
            setCallId(invocation.op, callId);

            // Then we try to put the invocation in the lowInvocations-array.
            int invocationIndex = lowInvocationIndex(callId);
            if (lowInvocations.compareAndSet(invocationIndex, null, invocation)) {
                // We are lucky since we managed to set the invocation in the slot.
                // At this point the invocation is visible to other threads.
                return;
            }

            // Another invocation already took the slot, we need to look for a different slot.
            // To prevent spinning indefinitely,
            iteration++;

            if (iteration == 1) {
                // on the first iteration we
                remainingTimeoutMs = min(backoffTimeoutMs, invocation.op.getCallTimeout());
            }

            remainingTimeoutMs = idle(iteration, remainingTimeoutMs);

            if (remainingTimeoutMs <= 0) {
                // todo: improve message to include timeout.
                throw new HazelcastOverloadException("Failed to find an invocation slot for invocation:" + invocation);
            }

            setCallTimeout(invocation.op, remainingTimeoutMs);
        }
    }

    /**
     * Gets the partition id from an invocation.
     *
     * In case of a non partition specific call, a random partition-id is selected.
     *
     * @param invocation the invocation
     * @return the partition-id.
     */
    private int partitionId(Invocation invocation) {
        int partitionId = invocation.op.getPartitionId();

        if (partitionId >= 0) {
            // it is a partition-specific call
            return partitionId;
        } else {
            // it is a target specific call. So we pick a random partition.
            Random random = ThreadLocalRandom.current();
            return random.nextInt(partitionCount);
        }
    }

    /**
     * Returns the index of the call-id generator in the {@link #lowSequences} for a given partitionId.
     *
     * This index does not include the padding. To determine the real index in the array, it needs to be multiplied by
     * {@link #SEQUENCE_ALIGNMENT}.
     */
    private int lowSequenceIndex(long partitionId) {
        return (int) QuickMath.modPowerOfTwo(partitionId, concurrencyLevel);
    }


    /**
     * Retrieves the next low priority call id.
     *
     * @param sequenceIndex the index of the callId sequence.
     * @return the created call id.
     */
    private long newLowCallId(int sequenceIndex) {
        int offset = sequenceIndex * SEQUENCE_ALIGNMENT;

        for (; ; ) {
            long callId = lowSequences.addAndGet(offset, concurrencyLevel);

            if (callId > 0) {
                return callId;
            }

            // the counter has wrapped, we try to reset the counter and try again.
            // Resetting the counter is simple since the initial value is equal to the sequenceIndex
            lowSequences.compareAndSet(offset, callId, sequenceIndex);
        }
    }

    /**
     * Idles the current thread.
     *
     * This needs to get replaced with the IdleStrategy from HZ-Enterprise.
     *
     * @return the remaining timeout in millis.
     */
    public long idle(int iteration, long remainingTimeoutMs) {
        if (iteration < 50) {
            // first we round we just retry
            return remainingTimeoutMs;
        }

        if (iteration < 1000) {
            // then we start to yield.
            Thread.yield();
            return remainingTimeoutMs;
        }

        long maximumDelayMs = min(remainingTimeoutMs, MAX_DELAY_MS);

        int correctedIteration = iteration - 999;
        long foo = round(pow(1.3, correctedIteration));
        long currentDelayMs = min(maximumDelayMs, foo);

        // then we calculate the actual delay. We add randomization to reduce the chance of starvation for threads that
        // have been idling for some time. Without randomization, a thread that has been idling for some time will always
        // have a longer delay than threads that have been idling shorter.
        Random random = ThreadLocalRandom.current();
        long actualDelayMs = round(random.nextDouble() * currentDelayMs);

        System.out.println("actualDelayMs:" + actualDelayMs
                + " iteration:" + iteration
                + " correctedIteration:" + correctedIteration
                + " foo:" + foo
                + " remainingTimeout:" + remainingTimeoutMs
                + " backoffTimeoutMs:" + backoffTimeoutMs);

        parkNanos(MILLISECONDS.toNanos(actualDelayMs));
        backoffTotalMs.inc(actualDelayMs);
        return remainingTimeoutMs - actualDelayMs;
    }

    // ======================== deregister ==================================

    /**
     * Deregisters an invocation.
     *
     * todo: this call can be made by multiple threads:
     * - response thread
     * - monitor thread
     * This can lead to problems because I doubt if invocation is threadsafe.
     *
     * If the invocation registration was skipped, the call is ignored.
     *
     * If the invocation already has been unregistered, the call is ignored.
     *
     * @param invocation The Invocation to deregister.
     */
    public void deregister(Invocation invocation) {
        if (invocation.op.getCallId() < 0) {
            deregisterHigh(invocation);
        } else {
            deregisterLow(invocation);
        }
    }

    private void deregisterHigh(Invocation invocation) {
        highInvocations.remove(invocation.op.getCallId());
        setCallId(invocation.op, 0);
    }

    private void deregisterLow(Invocation invocation) {
        long callId = invocation.op.getCallId();

        // todo: there is a potential race here since this could be called concurrently
        // todo: why do we need to set the counter to 0?
        setCallId(invocation.op, 0);

        int index = lowInvocationIndex(callId);

        boolean success = lowInvocations.compareAndSet(index, invocation, null);

        if (!success && logger.isFinestEnabled()) {
            logger.finest("failed to deregister callId: " + callId + " " + invocation + ", no invocation was found.");
        }
    }

    // ==================================================================

    /**
     * Gets the index in the {@link #lowInvocations} for a low priority invocation.
     *
     * The passed in callId should be positive.
     *
     * @param callId the callId of the invocation.
     * @return the index.
     */
    int lowInvocationIndex(long callId) {
        return (int) QuickMath.modPowerOfTwo(callId, lowInvocationsLength);
    }


    /**
     * Returns the number of pending invocations.
     *
     * The size operation is relatively expensive since it needs to scan through the lowInvocationsLength.
     * This should be quite fast since the prefetcher has a predictable pattern it can deal with.
     *
     * @return the number of pending invocations.
     */
    public int size() {
        return highInvocations.size() + getPendingLowPriorityInvocations();
    }

    /**
     * Calculates the number of pending low priority invocations.
     *
     * @return the number of pending low priority invocations.
     */
    @Probe(name = "invocations.pending[lowPriority]", level = MANDATORY)
    private int getPendingLowPriorityInvocations() {
        int result = 0;
        for (int k = 0; k < lowInvocationsLength; k++) {
            if (lowInvocations.get(k) != null) {
                result++;
            }
        }
        return result;
    }

    /**
     * The returned iterator is on best effort. Normally this method is called for scanning the invocations,
     * so it doesn't need to be perfectly consistent.
     *
     * todo: what about duplicates?
     *
     * @return
     */
    @Override
    public Iterator<Invocation> iterator() {
        return new InvocationIterator();
    }

    public void reset() {
        for (Invocation invocation : this) {
            try {
                invocation.notifyError(new MemberLeftException());
            } catch (Throwable e) {
                logger.warning(invocation + " could not be notified with reset message -> " + e.getMessage());
            }
        }
    }

    public void shutdown() {
        for (Invocation invocation : this) {
            try {
                invocation.notifyError(new HazelcastInstanceNotActiveException());
            } catch (Throwable e) {
                logger.warning(invocation + " could not be notified with shutdown message -> " + e.getMessage(), e);
            }
        }
    }

    private class InvocationIterator implements Iterator<Invocation> {

        private int index = -1;
        private Invocation invocation;
        private Iterator<Invocation> iterator;

        @Override
        public boolean hasNext() {
            if (iterator != null) {
                return iterator.hasNext();
            }

            if (invocation != null) {
                return true;
            }

            for (; ; ) {
                index++;

                if (index >= lowInvocations.length()) {
                    break;
                }

                invocation = lowInvocations.get(index);
                if (invocation != null) {
                    return true;
                }
            }

            // todo: we could make use of an empty iterator if the highInvocations is empty
            // this prevents creation of litter.
            iterator = highInvocations.values().iterator();
            return iterator.hasNext();
        }

        @Override
        public Invocation next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            if (iterator != null) {
                return iterator.next();
            }

            Invocation tmp = invocation;
            invocation = null;
            return tmp;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
