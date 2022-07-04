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

package com.hazelcast.internal.util.collection;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.AbstractSet;
import java.util.BitSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;

/**
 * A mutable set of partition IDs. Since partition IDs are integers in a
 * range [0..partitionCount), a set of partition IDs can be efficiently
 * represented by a fixed-size {@link java.util.BitSet} whose value at a particular
 * index is set when that partition ID is present in the set. This offers better
 * performance than a general purpose {@code Set<Integer>} implementation and better
 * memory efficiency in most cases.
 * <p>
 * Additionally, the {@code PartitionIdSet} supplies specialized methods for {@code add},
 * {@code remove} and {@code contains} of primitive {@code int} arguments, as well as a
 * primitive {@code int} iterator implementation, to allow clients to avoid the cost of
 * boxing and unboxing.
 * <p>
 * This set's {@link PartitionIdSet#iterator() iterator} is a view of the actual
 * set, so any changes on the set will be reflected in the iterator and vice versa.
 * <p>
 * This class is not thread-safe.
 */
public class PartitionIdSet extends AbstractSet<Integer> implements IdentifiedDataSerializable {

    private static final int SIZE_UNKNOWN = -1;

    private int partitionCount;
    private BitSet bitSet;
    private int size = SIZE_UNKNOWN;

    // for deserialization
    PartitionIdSet() { }

    public PartitionIdSet(int partitionCount) {
        this(partitionCount, new BitSet(partitionCount));
    }

    /**
     * Creates a PartitionIdSet initially containing a sole partition ID.
     */
    public PartitionIdSet(int partitionCount, int solePartitionId) {
        this(partitionCount);
        bitSet.set(solePartitionId);
    }

    public PartitionIdSet(int partitionCount, Collection<Integer> initialPartitionIds) {
        this(partitionCount);
        for (int partitionId : initialPartitionIds) {
            bitSet.set(partitionId);
        }
    }

    public PartitionIdSet(int partitionCount, int[] initialPartitionIds) {
        this(partitionCount);
        for (int partitionId : initialPartitionIds) {
            bitSet.set(partitionId);
        }
    }

    public PartitionIdSet(PartitionIdSet initialPartitionIds) {
        this(initialPartitionIds.partitionCount, initialPartitionIds);
    }

    PartitionIdSet(int partitionCount, BitSet bitSet) {
        this.partitionCount = partitionCount;
        this.bitSet = bitSet;
    }

    @Override
    public Iterator<Integer> iterator() {
        return new PartitionIdSetIterator();
    }

    public PrimitiveIterator.OfInt intIterator() {
        return new PartitionIdSetIterator();
    }

    /**
     * Return the number of partitions in the set.
     */
    @Override
    public int size() {
        int s = size;
        if (s == SIZE_UNKNOWN) {
            s = bitSet.cardinality();
            size = s;
        }

        return s;
    }

    private void resetSize() {
        size = SIZE_UNKNOWN;
    }

    @Override
    public boolean isEmpty() {
        return bitSet.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        if (!(o instanceof Integer)) {
            throw new ClassCastException("PartitionIdSet can be only used with Integers");
        }
        return bitSet.get((Integer) o);
    }

    public boolean contains(int partitionId) {
        return bitSet.get(partitionId);
    }

    /**
     * @return {@code true} when {@code this} set contains all partition IDs contained in {@code other}
     */
    public boolean containsAll(PartitionIdSet other) {
        BitSet clone = (BitSet) bitSet.clone();
        clone.and(other.bitSet);
        return clone.cardinality() == other.bitSet.cardinality();
    }

    @Override
    public boolean add(Integer partitionId) {
        return add(partitionId.intValue());
    }

    public boolean add(int partitionId) {
        if (!bitSet.get(partitionId)) {
            bitSet.set(partitionId);
            resetSize();
            return true;
        } else {
            return false;
        }
    }

    public void addAll(PartitionIdSet other) {
        bitSet.or(other.bitSet);
        resetSize();
    }

    @Override
    public boolean remove(Object o) {
        if (!(o instanceof Integer)) {
            throw new ClassCastException("PartitionIdSet can be only used with Integers");
        }
        return remove(((Integer) o).intValue());
    }

    public boolean remove(int partitionId) {
        if (bitSet.get(partitionId)) {
            bitSet.clear(partitionId);
            resetSize();
            return true;
        } else {
            return false;
        }
    }

    public void removeAll(PartitionIdSet other) {
        other.bitSet.stream().forEach(bitSet::clear);
        resetSize();
    }

    @Override
    public void clear() {
        bitSet.clear();
        resetSize();
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    /**
     * Returns whether the intersection of this set with given argument is not empty.
     *
     * @return {@code true} when the intersection of {@code this} and {@code other} set
     * is not empty, otherwise {@code false}.
     */
    public boolean intersects(PartitionIdSet other) {
        return bitSet.intersects(other.bitSet);
    }

    /**
     * Intersects this set with the {@code other} set and returns the result as
     * a new set. Doesn't mutate this or the {@code other} instance.
     */
    public PartitionIdSet intersectCopy(PartitionIdSet other) {
        BitSet newBitSet = bitSetCopy();
        newBitSet.and(other.bitSet);
        return new PartitionIdSet(partitionCount, newBitSet);
    }

    /**
     * Mutates this {@code PartitionIdSet} so it contains the union of this and {@code other}'s
     * partition IDs.
     */
    public void union(PartitionIdSet other) {
        this.bitSet.or(other.bitSet);
        resetSize();
    }

    /**
     * Mutates this set so it contains its complement with respect to the universe of all partition IDs.
     */
    public void complement() {
        bitSet.flip(0, partitionCount);
        resetSize();
    }

    /**
     * @return {@code false} when all partition IDs in [0, partitionCount) are contained in this
     * set, otherwise {@code true}
     */
    public boolean isMissingPartitions() {
        return bitSet.nextClearBit(0) < partitionCount;
    }

    public PartitionIdSet copy() {
        return new PartitionIdSet(partitionCount, (BitSet) bitSet.clone());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PartitionIdSet other = (PartitionIdSet) o;

        return partitionCount == other.partitionCount && bitSet.equals(other.bitSet);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + partitionCount;
        result = 31 * result + bitSet.hashCode();
        return result;
    }

    /**
     * Returns the sole partition ID, if this set has a size of 1. Otherwise returns -1.
     */
    public int solePartition() {
        int candidateResult = bitSet.nextSetBit(0);
        if (bitSet.nextSetBit(candidateResult + 1) < 0) {
            return candidateResult;
        } else {
            return -1;
        }
    }

    public BitSet bitSetCopy() {
        return (BitSet) bitSet.clone();
    }

    @Override
    public int getFactoryId() {
        return UtilCollectionSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return UtilCollectionSerializerHook.PARTITION_ID_SET;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(partitionCount);
        out.writeLongArray(bitSet.toLongArray());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        partitionCount = in.readInt();
        bitSet = BitSet.valueOf(in.readLongArray());
    }

    private final class PartitionIdSetIterator
            implements PrimitiveIterator.OfInt {

        private int last = -1;
        private int next = -1;

        PartitionIdSetIterator() {
            moveNext();
        }

        private void moveNext() {
            last = next;
            next = bitSet.nextSetBit(next + 1);
        }

        @Override
        public boolean hasNext() {
            return next >= 0;
        }

        @Override
        public Integer next() {
            return nextInt();
        }

        @Override
        public int nextInt() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more elements");
            }
            int res = next;
            moveNext();
            return res;
        }

        @Override
        public void remove() {
            PartitionIdSet.this.remove(last);
        }
    }

}
