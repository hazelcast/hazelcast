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

package com.hazelcast.ringbuffer;

/**
 * The result of a
 * {@link Ringbuffer#readManyAsync(long, int, int, com.hazelcast.core.IFunction)}
 * operation.
 * <p>
 * Important:<br>
 * If an item is retrieved multiple times from the result set, a new
 * instance is returned for every invocation. This is done to prevent
 * unexpected sharing if the {@link java.util.concurrent.CompletionStage}
 * is shared between multiple threads.
 *
 * @param <E> item type
 * @since 3.5
 */
public interface ReadResultSet<E> extends Iterable<E> {

    /**
     * Value returned from methods returning a sequence number when the
     * information is not available (e.g. because of rolling upgrade and some
     * members not returning the sequence).
     */
    int SEQUENCE_UNAVAILABLE = -1;

    /**
     * Returns the number of items that have been read before filtering.
     * <p>
     * If no filter is set, then the {@code readCount} will be equal to
     * {@link #size}.
     * But if a filter is applied, it could be that items are read, but are
     * filtered out. So if you are trying to make another read based on the
     * {@link ReadResultSet} then you should increment the sequence by
     * {@code readCount} and not by {@link #size()}.
     * Otherwise you will be re-reading the same filtered messages.
     *
     * @return the number of items read (including the filtered ones).
     */
    int readCount();

    /**
     * Gets the item at the given index.
     *
     * @param index the index
     * @return the found item.
     * @throws IllegalArgumentException if index out of bounds.
     */
    E get(int index);

    /**
     * Return the sequence number for the item at the given index.
     *
     * @param index the index
     * @return the sequence number for the ringbuffer item
     * @throws IllegalArgumentException if index out of bounds.
     * @see com.hazelcast.internal.cluster.ClusterService#getClusterVersion()
     * @since 3.9
     */
    long getSequence(int index);

    /**
     * Return the result set size. See also {@link #readCount()}.
     *
     * @return the result set size
     * @since 3.9
     */
    int size();

    /**
     * Returns the sequence of the item following the last read item. This
     * sequence can then be used to read items following the ones returned by
     * this result set.
     * Usually this sequence is equal to the sequence used to retrieve this
     * result set incremented by the {@link #readCount()}. In cases when the
     * reader tolerates lost items, this is not the case.
     * For instance, if the reader requests an item with a stale sequence (one
     * which has already been overwritten), the read will jump to the oldest
     * sequence and read from there.
     * Similarly, if the reader requests an item in the future (e.g. because
     * the partition was lost and the reader was unaware of this), the read
     * method will jump back to the newest available sequence.
     * Because of these jumps and only in the case when the reader is loss
     * tolerant, the next sequence must be retrieved using this method.
     * A return value of {@value SEQUENCE_UNAVAILABLE} means that the
     * information is not available.
     *
     * @return the sequence of the item following the last item in the result set
     * @since 3.10
     */
    long getNextSequenceToReadFrom();
}
