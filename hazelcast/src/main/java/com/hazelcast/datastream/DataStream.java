/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.datastream;


import com.hazelcast.core.DistributedObject;

import java.util.List;

/**
 * todo:
 * - option to read from the stream
 * - aggregation needs to have same type field. Which
 *
 * there is an issue with tenuring; what does it exactly mean.
 * tenuring is the age when a region becomes immutable. but once it has tenured, it can either be deleted,
 * or be made durable and then deleted.
 *
 * @param <R>
 */
public interface DataStream<R> extends DistributedObject {

    DataOutputStream<R> newOutputStream();

    /**
     * DataInputStream that receives data for a single partition.
     *
     * @param partitionId
     * @param offset
     * @return
     */
    DataInputStream<R> newInputStream(int partitionId, long offset);

    /**
     * DataInputStream that receives data for the given partitions.
     * <p>
     * Input
     *
     * @param partitions
     * @param offsets
     * @return
     */
    DataInputStream<R> newInputStream(List<Integer> partitions, List<Long> offsets);

    /**
     * DataInputStream that receives data for all partitions.
     *
     * @param offsets
     * @param <P>
     * @return
     */
    <P> DataInputStream<R> newInputStream(List<Long> offsets);

    /**
     * Gets the tail for the given partition.
     * <p>
     * The tail is the side where items are added to.
     * <p>
     * The tail is the offset to the first available byte.
     *
     * @param partitionKey
     * @param <P>
     * @return the tail.
     */
    <P> long tail(P partitionKey);

    /**
     * Returns the head for the given partition.
     * <p>
     * The head is pointing to the oldest byte.
     * <p>
     * If head and tail are equal, no messages are available in the partition.
     *
     * @param partitionKey
     * @param <P>
     * @return the tail.
     */
    <P> long head(P partitionKey);

    /**
     * Returns a DataFrame representation of this DataStream.
     *
     * @return the DataFrame.
     */
    DataFrame<R> asFrame();
}
