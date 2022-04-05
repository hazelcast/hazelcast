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

package com.hazelcast.internal.util;

import com.hazelcast.internal.util.collection.ImmutablePartitionIdSet;
import com.hazelcast.internal.util.collection.PartitionIdSet;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Utility class for Sets.
 */
public final class SetUtil {

    private static final float HASHSET_DEFAULT_LOAD_FACTOR = 0.75f;

    private SetUtil() {
    }

    /**
     * Utility method that creates an {@link java.util.HashSet} with its initialCapacity calculated
     * to minimize rehash operations
     */
    public static <E> Set<E> createHashSet(int expectedMapSize) {
        final int initialCapacity = (int) (expectedMapSize / HASHSET_DEFAULT_LOAD_FACTOR) + 1;
        return new HashSet<E>(initialCapacity, HASHSET_DEFAULT_LOAD_FACTOR);
    }

    /**
     * Utility method that creates an {@link java.util.LinkedHashSet} with its initialCapacity calculated
     * to minimize rehash operations
     */
    public static <E> Set<E> createLinkedHashSet(int expectedMapSize) {
        final int initialCapacity = (int) (expectedMapSize / HASHSET_DEFAULT_LOAD_FACTOR) + 1;
        return new LinkedHashSet<E>(initialCapacity, HASHSET_DEFAULT_LOAD_FACTOR);
    }

    public static PartitionIdSet singletonPartitionIdSet(int partitionCount, int partitionId) {
        PartitionIdSet set = new PartitionIdSet(partitionCount);
        set.add(partitionId);
        return set;
    }

    public static PartitionIdSet immutablePartitionIdSet(int partitionCount, Collection<Integer> partitionIds) {
        return new ImmutablePartitionIdSet(partitionCount, partitionIds);
    }

    public static PartitionIdSet allPartitionIds(int partitionCount) {
        PartitionIdSet set = new PartitionIdSet(partitionCount);
        set.complement();
        return set;
    }
}
