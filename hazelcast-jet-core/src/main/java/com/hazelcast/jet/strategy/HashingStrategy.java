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

package com.hazelcast.jet.strategy;

import com.hazelcast.jet.container.ContainerDescriptor;

import java.io.Serializable;

/**
 * Strategy to calculate hash for data passed between JET containers
 *
 * Used for example to determine correct task which should consume data
 *
 * For example object can be a tuple, partitionKey fields to be used for hash calculation
 *
 * @param <O> type of the input object
 * @param <K> type of the partitionKey
 */
public interface HashingStrategy<O, K> extends Serializable {

    /**
     * Calculates the hash for a given object and partition key
     * @param object the object to calculate the hash for
     * @param partitionKey the partition key for the given object, calculated by the
     *                     given @{code PartitioningStrategy}
     * @param containerDescriptor context for the container where the hash is calculated
     * @return the calculated hash for the object
     */
    int hash(O object, K partitionKey, ContainerDescriptor containerDescriptor);
}
