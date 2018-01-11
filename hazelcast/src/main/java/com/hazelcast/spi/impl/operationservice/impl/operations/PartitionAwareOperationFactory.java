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

package com.hazelcast.spi.impl.operationservice.impl.operations;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Creates partition specific operations.
 * <p>
 * Intended to be used by {@link PartitionIteratingOperation}.
 */
public abstract class PartitionAwareOperationFactory implements OperationFactory {

    /**
     * Partition-operations will be created for these partition IDs.
     */
    protected int[] partitions;

    /**
     * This method will be called on operation runner node.
     * <p>
     * If {@link PartitionAwareOperationFactory} needs to have runner-side state different from caller-side one,
     * this method can be used to create it. Otherwise, stateful factories may cause JMM problems.
     *
     * @param nodeEngine nodeEngine
     */
    public PartitionAwareOperationFactory createFactoryOnRunner(NodeEngine nodeEngine) {
        return this;
    }

    /**
     * This method can be called both caller and runner nodes.
     * <p>
     * Creates a partition-operation for supplied partition ID.
     *
     * @param partition ID of partition
     * @return created partition-operation
     */
    public abstract Operation createPartitionOperation(int partition);

    /**
     * This method will be called on operation runner node.
     * <p>
     * Created operations by this factory will be run on the partitions returned by this method.
     * Returning {@code null} means operations will be run provided partitions by default.
     *
     * @return {@code null} to preserve default behaviour or return relevant partition IDs for the operations of this factory
     */
    @SuppressFBWarnings("EI_EXPOSE_REP")
    public int[] getPartitions() {
        return partitions;
    }

    @Override
    public Operation createOperation() {
        throw new UnsupportedOperationException("Use createPartitionOperation() with PartitionAwareOperationFactory");
    }
}
