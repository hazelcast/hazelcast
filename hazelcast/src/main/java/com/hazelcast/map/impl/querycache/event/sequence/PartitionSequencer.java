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

package com.hazelcast.map.impl.querycache.event.sequence;

/**
 * General contract of a sequence-number provider for a partition.
 * <p>
 * Each partition has its own sequence-number on publisher-side. For every generated event, this sequence will be
 * incremented by one and set to that event before sending it to the subscriber side.
 * <p>
 * On subscriber side, this sequence is used to keep track of published events; upon arrival of an event, there
 * is a pre-condition check to decide whether that the arrived event has the next-expected-sequence-number for the
 * relevant partition. If the event has next-sequence, it is applied to the query-cache.
 * <p>
 * Implementations of this interface should be thread-safe.
 */
public interface PartitionSequencer {

    /**
     * Returns next number in sequence for the partition.
     *
     * @return next number in sequence.
     */
    long nextSequence();

    /**
     * Sets the current sequence number for the partition.
     *
     * @param update new sequence number.
     */
    void setSequence(long update);

    /**
     * Atomically sets the value of sequence number for the partition.
     *
     * @param expect the expected sequence.
     * @param update the new sequence.
     * @return {@code true} if Compare-and-Set operation is successful, otherwise returns {@code false}.
     */
    boolean compareAndSetSequence(long expect, long update);

    /**
     * Returns current sequence number of the partition.
     *
     * @return current sequence number.
     */
    long getSequence();

    /**
     * Resets {@link PartitionSequencer}.
     * After this call returns sequence generation will be start from zero.
     */
    void reset();
}
