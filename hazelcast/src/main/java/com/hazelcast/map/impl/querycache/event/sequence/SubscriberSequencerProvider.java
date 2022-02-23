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
 * Provides sequences for subscriber side.
 *
 * @see PartitionSequencer
 */
public interface SubscriberSequencerProvider {

    /**
     * Atomically sets the value of sequence number for the partition.
     *
     * @param expect      the expected sequence.
     * @param update      the new sequence.
     * @param partitionId ID of the partition.
     * @return {@code true} if Compare-and-Set operation
     * is successful, otherwise returns {@code false}.
     */
    boolean compareAndSetSequence(long expect, long update, int partitionId);

    /**
     * Returns partition's current sequence number.
     *
     * @param partitionId ID of the partition.
     * @return current sequence number.
     */
    long getSequence(int partitionId);

    /**
     * Resets the sequence number for the supplied {@code partition} to zero.
     *
     * @param partitionId ID of the partition.
     */
    void reset(int partitionId);

    /**
     * Like {@link #reset(int)} but this method resets all partition sequences.
     */
    void resetAll();
}
