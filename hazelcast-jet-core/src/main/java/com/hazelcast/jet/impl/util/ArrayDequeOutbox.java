/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.util;

import com.hazelcast.jet.Outbox;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;

/**
 * Implements {@code Outbox} with an array of {@link ArrayDeque}s.
 */
public final class ArrayDequeOutbox implements Outbox {

    private final Queue<Object>[] buckets;
    private final int[] capacities;
    private final ProgressTracker progTracker;

    public ArrayDequeOutbox(int size, int[] capacities, ProgressTracker progTracker) {
        this.capacities = capacities.clone();
        this.buckets = new Queue[size];
        this.progTracker = progTracker;
        Arrays.setAll(buckets, i -> new ArrayDeque());
    }

    @Override
    public int bucketCount() {
        return buckets.length;
    }

    @Override
    public boolean offer(int ordinal, @Nonnull Object item) {
        if (ordinal != -1) {
            if (isBucketFull(ordinal)) {
                return false;
            }
            buckets[ordinal].add(item);
            progTracker.madeProgress();
            return true;
        }
        for (int i = 0; i < buckets.length; i++) {
            if (isBucketFull(i)) {
                return false;
            }
        }
        for (Queue<Object> bucket : buckets) {
            bucket.add(item);
        }
        progTracker.madeProgress();
        return true;
    }

    @Override
    public boolean offer(int[] ordinals, @Nonnull Object item) {
        for (int ord : ordinals) {
            if (isBucketFull(ord)) {
                return false;
            }
        }
        for (int ord : ordinals) {
            buckets[ord].add(item);
        }
        progTracker.madeProgress();
        return true;
    }

    private boolean isBucketFull(int ordinal) {
        return buckets[ordinal].size() >= capacities[ordinal];
    }


    // Private API for tasklets

    /**
     * Exposes individual buckets to the processor tasklets.
     * @param ordinal ordinal of the bucket
     */
    public Queue<Object> queueWithOrdinal(int ordinal) {
        return buckets[ordinal];
    }

    /**
     * Forcefully adds an item to all the buckets of this outbox, ignoring the
     * configured capacity limits. Introduced to simplify tasklet logic, which
     * calls it to add the final done item to the buckets.
     */
    public void addIgnoringCapacity(Object item) {
        for (Queue<Object> bucket : buckets) {
            bucket.add(item);
        }
    }
}
