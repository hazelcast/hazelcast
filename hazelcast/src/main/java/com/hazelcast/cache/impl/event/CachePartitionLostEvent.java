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

package com.hazelcast.cache.impl.event;

import com.hazelcast.cluster.Member;

/**
 * Used to provide information about the lost partition of a cache.
 *
 * @see CachePartitionLostEvent
 * @since 3.6
 */
public class CachePartitionLostEvent extends AbstractICacheEvent {

    private static final long serialVersionUID = -7445714640964238109L;

    private final int partitionId;

    public CachePartitionLostEvent(Object source, Member member, int eventType, int partitionId) {
        super(source, member, eventType);
        this.partitionId = partitionId;
    }

    /**
     * Returns the partition ID that has been lost for the given cache.
     *
     * @return the partition ID that has been lost for the given cache.
     */
    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + '{'
                + super.toString()
                + ", partitionId=" + partitionId
                + '}';
    }
}
