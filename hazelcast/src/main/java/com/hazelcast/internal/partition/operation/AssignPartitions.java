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

package com.hazelcast.internal.partition.operation;

import com.hazelcast.internal.partition.MigrationCycleOperation;
import com.hazelcast.internal.partition.PartitionRuntimeState;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;

/** Sent from non-master nodes to the master to initialize the partition assignment. */
public class AssignPartitions extends AbstractPartitionOperation implements MigrationCycleOperation {

    private PartitionRuntimeState partitionState;

    @Override
    public void run() {
        InternalPartitionServiceImpl service = getService();
        partitionState = service.firstArrangement();
    }

    @Override
    public Object getResponse() {
        return partitionState;
    }

    @Override
    public int getId() {
        return PartitionDataSerializerHook.ASSIGN_PARTITIONS;
    }
}
