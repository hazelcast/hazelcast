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

package com.hazelcast.map;

import com.hazelcast.cluster.Member;

/**
 * Used for providing information about the lost partition for a map
 *
 * @see com.hazelcast.map.listener.MapPartitionLostListener
 */
public class MapPartitionLostEvent extends AbstractIMapEvent {

    private static final long serialVersionUID = -7445734640964238109L;

    private final int partitionId;

    public MapPartitionLostEvent(Object source, Member member, int eventType, int partitionId) {
        super(source, member, eventType);
        this.partitionId = partitionId;
    }

    /**
     * Returns the partition ID that has been lost for the given map
     *
     * @return the partition ID that has been lost for the given map
     */
    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{"
                + super.toString()
                + ", partitionId=" + partitionId
                + '}';
    }
}
