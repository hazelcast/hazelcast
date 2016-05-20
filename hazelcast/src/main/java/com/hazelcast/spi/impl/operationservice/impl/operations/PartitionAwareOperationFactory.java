/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.operationservice.impl.operations;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

/**
 * Provides a way to instantiate partition specific operations in the {@link PartitionIteratingOperation}
 */
public interface PartitionAwareOperationFactory extends OperationFactory {

    /**
     * Initializes this factory.
     *
     * @param nodeEngine nodeEngine
     */
    void init(NodeEngine nodeEngine);

    /**
     * Create a partition-operation for the supplied partition-id
     *
     * @param partition id of partition
     * @return new operation
     */
    Operation createPartitionOperation(int partition);

    /**
     * Created operations by this factory will be run on these partitions.
     * Return null to preserve  default behaviour.
     *
     * @return null to preserve default behaviour or return all partition-ids for the operations of this factory.
     */
    int[] getPartitions();
}
