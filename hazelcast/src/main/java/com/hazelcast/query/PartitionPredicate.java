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

package com.hazelcast.query;

import com.hazelcast.internal.serialization.BinaryInterface;

/**
 * A {@link Predicate} that restricts the execution of a {@link Predicate} to a single partition.
 *
 * This can help to speed up query execution since only a single instead of all partitions needs to be queried.
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
     * Returns the partition key that determines the partition the {@linkplain
     * #getTarget() target} {@link Predicate} is going to execute on.
     *
     * @return the partition key
     */
    Object getPartitionKey();

    /**
     * Returns the target {@link Predicate}.
     */
    Predicate<K, V> getTarget();
}
