/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.mapreduce;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.BinaryInterface;

/**
 * An implementation of this interface contains current information about
 * the status of an process piece while operation is executing.
 *
 * @since 3.2
 * @deprecated MapReduce is deprecated and will be removed in 4.0.
 * For map aggregations, you can use {@link com.hazelcast.aggregation.Aggregator} on IMap.
 * For general data processing, it is superseded by <a href="http://jet.hazelcast.org">Hazelcast Jet</a>.
 */
@Deprecated
@BinaryInterface
public interface JobPartitionState {

    /**
     * Returns the owner of this partition
     *
     * @return owner of the partition
     */
    Address getOwner();

    /**
     * Returns the current processing state of this partition
     *
     * @return processing state of the partition
     */
    State getState();

    /**
     * Definition of the processing states
     */
    enum State {
        /**
         * Partition waits for being calculated
         */
        WAITING,

        /**
         * Partition is in mapping phase
         */
        MAPPING,

        /**
         * Partition is in reducing phase (mapping may still
         * not finished when this state is reached since there
         * is a chunked based operation underlying)
         */
        REDUCING,

        /**
         * Partition is fully processed
         */
        PROCESSED,

        /**
         * Partition calculation cancelled due to an internal exception
         */
        CANCELLED;

        /**
         * Returns an processing state by its given ordinal
         *
         * @param ordinal ordinal to search for
         * @return the processing state for the given ordinal
         */
        public static State byOrdinal(int ordinal) {
            for (State state : values()) {
                if (state.ordinal() == ordinal) {
                    return state;
                }
            }
            return null;
        }
    }

}
