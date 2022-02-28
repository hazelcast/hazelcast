/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.ringbuffer.impl;

import java.util.Arrays;

import static com.hazelcast.internal.util.Clock.currentTimeMillis;

/**
 * The expiration policy for the ring buffer items. Currently keeps the expiration times in an array of longs.
 */
final class RingbufferExpirationPolicy {

    // contains the expiration time in ms when the item should be expired.
    long[] ringExpirationMs;
    private final long ttlMs;

    RingbufferExpirationPolicy(long capacity, long ttlMs) {
        this.ringExpirationMs = new long[(int) capacity];
        this.ttlMs = ttlMs;
    }

    /**
     * Cleans up the ringbuffer by deleting all expired items.
     */
    @SuppressWarnings("unchecked")
    void cleanup(Ringbuffer ringbuffer) {
        if (ringbuffer.headSequence() > ringbuffer.tailSequence()) {
            return;
        }

        long now = currentTimeMillis();
        while (ringbuffer.headSequence() <= ringbuffer.tailSequence()) {
            final long headSequence = ringbuffer.headSequence();

            if (ringExpirationMs[toIndex(headSequence)] > now) {
                return;
            }

            // we null the slot and allow the gc to take care of the object.
            // if we don't clean it, we'll have a potential memory leak.
            ringbuffer.set(headSequence, null);

            // we don't need to 0 the ringExpirationMs slot since it contains a long value.

            // and we move the head to the next item.
            // if nothing remains in the ringbuffer, then the head will be 1 larger than the tail.
            ringbuffer.setHeadSequence(ringbuffer.headSequence() + 1);
        }
    }

    int toIndex(long sequence) {
        return (int) (sequence % ringExpirationMs.length);
    }

    /**
     * Set the expiration value at the target sequence to the current time plus the TTL.
     *
     * @param sequence the sequence for which the expiration is set
     */
    void setExpirationAt(long sequence) {
        setExpirationAt(sequence, currentTimeMillis() + ttlMs);
    }

    /**
     * Get the expiration value for the sequence.
     *
     * @param seq the sequence for which the expiration is needed
     * @return the time in milliseconds
     */
    long getExpirationAt(long seq) {
        return ringExpirationMs[toIndex(seq)];
    }

    /**
     * Set the expiration value at the target sequence.
     *
     * @param seq   the sequence for which the expiration is set
     * @param value the value (time) at which the item is expired
     */
    void setExpirationAt(long seq, long value) {
        ringExpirationMs[toIndex(seq)] = value;
    }

    /**
     * Get the TTL in milliseconds.
     *
     * @return the TTL
     */
    long getTtlMs() {
        return ttlMs;
    }

    /**
     * Resets the expiration policy for all items.
     */
    public void clear() {
        Arrays.fill(ringExpirationMs, 0);
    }
}
