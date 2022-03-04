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
 * This interface should be implemented by events which need a sequence number.
 */
public interface Sequenced {

    /**
     * Returns the sequence number.
     *
     * @return sequence number.
     */
    long getSequence();

    /**
     * Returns partition ID which this sequence belongs to.
     *
     * @return partition ID which this sequence belongs to.
     */
    int getPartitionId();

    /**
     * Sets sequence.
     *
     * @param sequence the sequence number to be set.
     */
    void setSequence(long sequence);
}
