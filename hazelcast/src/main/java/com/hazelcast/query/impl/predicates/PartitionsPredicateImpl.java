/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl.predicates;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.BinaryInterface;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.PartitionsPredicate;
import com.hazelcast.query.Predicate;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Implementation of {@link PartitionPredicate}.
 *
 * @param <K> type of the entry key
 * @param <V> type of the entry value
 */
@BinaryInterface
public class PartitionsPredicateImpl<K, V> implements PartitionsPredicate<K, V>, IdentifiedDataSerializable {

    private static final long serialVersionUID = 1L;

    private Collection<? extends Object> partitionKeys;
    private Predicate<K, V> target;

    // should only be used for deserialization
    public PartitionsPredicateImpl() {
    }

    /**
     * Creates a new PartitionPredicate.
     *
     * @param partitionKey the partition key
     * @param target       the target {@link Predicate}
     * @throws NullPointerException     if partitionKey or target predicate is {@code null}
     */
    public PartitionsPredicateImpl(Collection<? extends Object> partitionKey, Predicate<K, V> target) {
        this.partitionKeys = checkNotNull(partitionKey, "partitionKeys can't be null");
        this.target = checkNotNull(target, "target predicate can't be null");
    }

    /**
     * Returns the partition key that determines the partition the target {@link Predicate} is going to execute on.
     *
     * @return the partition ID
     */
    @Override
    public Collection<? extends Object> getPartitionKeys() {
        return partitionKeys;
    }

    /**
     * Returns the target {@link Predicate}.
     *
     * @return the target {@link Predicate}.
     */
    @Override
    public Predicate<K, V> getTarget() {
        return target;
    }

    @Override
    public boolean apply(Map.Entry<K, V> mapEntry) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getFactoryId() {
        return PredicateDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return PredicateDataSerializerHook.PARTITIONS_PREDICATE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(partitionKeys);
        out.writeObject(target);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.partitionKeys = in.readObject();
        this.target = in.readObject();
    }

    @Override
    public String toString() {
        return "PartitionsPredicate{"
                + "partitionKeys=" + partitionKeys
                + ", target=" + target
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PartitionsPredicateImpl<?, ?> that = (PartitionsPredicateImpl<?, ?>) o;

        if (partitionKeys != null ? !partitionKeys.equals(that.partitionKeys) : that.partitionKeys != null) {
            return false;
        }
        return target != null ? target.equals(that.target) : that.target == null;
    }

    @Override
    public int hashCode() {
        int result = partitionKeys != null ? partitionKeys.hashCode() : 0;
        result = 31 * result + (target != null ? target.hashCode() : 0);
        return result;
    }
}
