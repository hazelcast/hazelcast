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

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.util.function.Supplier;

/**
 * A publisher for the {@link DataStream}.
 * <p>
 * todo: do we want to return the offset after the write? So we can identify the message?
 *
 * @param <R>
 */
public interface DataOutputStream<R> {

    /**
     * Writes the record on a random partition.
     * <p>
     * Writing to a random partition helps to spread load over the partitions, but there is
     * no guarantee on the order of processing of messages. If ordering between 2 messages
     * is relevant, the a write on the same partition needs to be done. But be warned:
     * if you write to a single partition, you can get imbalances.
     *
     * @param record the record to publish.
     * @return the byte-offset of the written record.
     * @throws NullPointerException if record is null.
     */
    long write(R record);

    /**
     * Asynchronously writes the record to a random partition.
     *
     * @param record the record to write.
     * @return the ICompletableFuture that has the byteoffset of the record written.
     * @see #write(Object) for more detail about this method.
     */
    ICompletableFuture<Long> writeAsync(R record);

    /**
     * Fills the Stream with data provided by the Supplier.
     * <p>
     * The main use-case for this method is testing. To load huge quantities of data
     * in memory can be very time consuming.
     *
     * @param count
     * @param supplier
     * @throws IllegalArgumentException if count smaller than 0.
     * @throws NullPointerException     if supplier is null.
     */
    void fill(long count, Supplier<R> supplier);

    /**
     * Pulls all values from the map into the DataStream.
     *
     * @param src
     * @throws NullPointerException if src is null.
     */
    void populate(IMap src);

    /**
     * Writes the record on the specified partition.
     *
     * @param partitionKey
     * @param record
     * @param <P>
     * @throws NullPointerException if record is null.
     */
    <P> long write(P partitionKey, R record);

    /**
     * Asynchronously writes the record on the specified partition.
     *
     * @param partitionKey
     * @param record
     * @param <P>
     * @return
     */
    <P> ICompletableFuture<Long> writeAsync(P partitionKey, R record);
}
