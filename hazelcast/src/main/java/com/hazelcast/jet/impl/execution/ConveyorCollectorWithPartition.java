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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.internal.util.concurrent.ConcurrentConveyor;
import com.hazelcast.jet.impl.util.ObjectWithPartitionId;
import com.hazelcast.jet.impl.util.ProgressState;

public class ConveyorCollectorWithPartition extends ConveyorCollector {

    public ConveyorCollectorWithPartition(ConcurrentConveyor<Object> conveyor, int queueIndex, int[] partitions) {
        super(conveyor, queueIndex, partitions);
    }

    @Override
    public ProgressState offer(Object item, int partitionId) {
        return offerToConveyor(new ObjectWithPartitionId(item, partitionId));
    }

    @Override
    public ProgressState offer(Object item) {
        return offer(item, -1);
    }
}
