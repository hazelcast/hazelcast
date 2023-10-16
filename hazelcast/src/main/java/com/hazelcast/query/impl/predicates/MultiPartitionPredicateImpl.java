/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkFalse;

import com.hazelcast.internal.serialization.BinaryInterface;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.PartitionPredicate;
import com.hazelcast.query.Predicate;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;

/**
 * Implementation of {@link PartitionPredicate}.
 *
 * @param <K> type of the entry key
 * @param <V> type of the entry value
 */
@BinaryInterface
public class MultiPartitionPredicateImpl<K, V> implements PartitionPredicate<K, V>, IdentifiedDataSerializable {

    private static final Random RANDOM = new Random();
    private Set<? extends Object> partitionKeys;
    private Predicate<K, V> target;

    // should only be used for deserialization
    public MultiPartitionPredicateImpl() {
    }

    /**
     * Creates a new MultiPartitionPredicate.
     *
     * @param partitionKeys the partition keys
     * @param target       the target {@link Predicate}
     * @throws NullPointerException     if partitionKey or target predicate is {@code null}
     */
    public MultiPartitionPredicateImpl(Set<? extends Object> partitionKeys, Predicate<K, V> target) {
        this.partitionKeys = checkNotNull(partitionKeys, "partitionKeys can't be null");
        checkFalse(partitionKeys.isEmpty(), "partitionKeys must not be empty");
        this.target = checkNotNull(target, "target predicate can't be null");
    }

    /**
     * Returns the partition keys that determines the partitions the target {@link Predicate} is going to execute on.
     *
     * @return the partition IDs
     */
    @Override
    public Collection<? extends Object> getPartitionKeys() {
        return partitionKeys;
    }

    /**
     * Returns a random partition key that can be used to pick which member of a cluster to execute on.
     *
     * If there is only a single partition key in {@linkplain #getPartitionKeys() partitionKeys} it is always returned immediately
     *
     * @return the randomly selected partition key from {@linkplain #getPartitionKeys()} partitionKeys}.
     */
    @Override
    public Object getPartitionKey() {
        if (partitionKeys.size() == 1) {
            return getSinglePartitionKey();
        }
        // Empty collections are rejected by the object constructor and serializer so this should be safe
        return partitionKeys.stream()
                .skip(RANDOM.nextInt(partitionKeys.size()))
                .findFirst()
                .get();
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
        return PredicateDataSerializerHook.MULTI_PARTITION_PREDICATE;
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
        return "MultiPartitionPredicate{"
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
        MultiPartitionPredicateImpl<?, ?> that = (MultiPartitionPredicateImpl<?, ?>) o;
        return Objects.equals(partitionKeys, that.partitionKeys)
               && Objects.equals(target, that.target);
    }

    @Override
    public int hashCode() {
        int result = partitionKeys != null ? partitionKeys.hashCode() : 0;
        result = 31 * result + (target != null ? target.hashCode() : 0);
        return result;
    }

    private Object getSinglePartitionKey() {
        for (Object key : partitionKeys) {
            return key;
        }
        throw new RuntimeException(
            "Unreachable branch. PartitionPredicateImpl constructor should check and throw if partitionKeys is empty"
        );
    }
}
