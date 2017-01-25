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
 * Implements {@code Outbox} with an {@link ArrayDeque}.
 */
public final class ArrayDequeOutbox implements Outbox {

    private final ArrayDeque<Object>[] buckets;
    private final int[] highWaterMarks;

    public ArrayDequeOutbox(int size, int[] highWaterMarks) {
        this.highWaterMarks = highWaterMarks.clone();
        this.buckets = new ArrayDeque[size];
        Arrays.setAll(buckets, i -> new ArrayDeque());
    }

    @Override
    public int bucketCount() {
        return buckets.length;
    }

    @Override
    public void add(int ordinal, @Nonnull Object item) {
        if (ordinal != -1) {
            buckets[ordinal].add(item);
        } else {
            for (ArrayDeque<Object> queue : buckets) {
                queue.add(item);
            }
        }
    }

    @Override
    public boolean isHighWater(int ordinal) {
        if (ordinal != -1) {
            return buckets[ordinal].size() >= highWaterMarks[ordinal];
        }
        for (int i = 0; i < buckets.length; i++) {
            if (buckets[i].size() >= highWaterMarks[i]) {
                return true;
            }
        }
        return false;
    }


    // Private API that exposes the ArrayDeques to the ProcessorTasklet

    public Queue<Object> queueWithOrdinal(int ordinal) {
        return buckets[ordinal];
    }
}
