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

package com.hazelcast.query;

import com.hazelcast.internal.serialization.BinaryInterface;
import java.util.Collection;
import java.util.Collections;

/**
 * A {@link Predicate} that restricts the execution of a {@link Predicate} to specific partitions.
 *
 * This can help to speed up query execution since only a subset of all partitions needs to be queried.
 *
 * This predicate only has effect if used as an outermost predicate.
 *
 * @param <K> type of the entry key
 * @param <V> type of the entry value
 * @see Predicates#partitionPredicate(Object, Predicate)
 */
@BinaryInterface
public interface PartitionPredicate<K, V> extends Predicate<K, V> {

    /**
     * Returns the partition keys that determine the partitions the {@linkplain
     * #getTarget() target} {@link Predicate} is going to execute on.
     *
     * A default implementation of {@linkplain #getPartitionKeys() partition keys}
     * that wraps the {@linkplain #getPartitionKey() partition key} in a singleton
     * collection is provided for backwards compatibility.
     *
     * @return the partition keys
     * @since 5.2
     */
    default Collection<? extends Object> getPartitionKeys() {
        return Collections.singleton(getPartitionKey());
    }

    /**
     * Returns a random partition key from the {@linkplain #getPartitionKeys() collection}.
     * This is useful for client message routing to cluster instances.
     * If there is a single value in the collection it is always returned as-is to be backwards
     * compatible with older versions of PartitionPredicate.
     *
     * @return the single key
     */
    Object getPartitionKey();

    /**
     * Returns the target {@link Predicate}.
     */
    Predicate<K, V> getTarget();
}
