/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.concurrent.atomiclong;

import com.hazelcast.core.AsyncAtomicLong;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.ExceptionUtil;

abstract class AtomicLongProxySupport extends AbstractDistributedObject<AtomicLongService> implements AsyncAtomicLong {

    final String name;
    final int partitionId;

    protected AtomicLongProxySupport(String name, NodeEngine nodeEngine, AtomicLongService service) {
        super(nodeEngine, service);
        this.name = name;
        this.partitionId = nodeEngine.getPartitionService().getPartitionId(getNameAsPartitionAwareData());
    }

    @Override
    public String getName() {
        return name;
    }

    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public String getServiceName() {
        return AtomicLongService.SERVICE_NAME;
    }

    <E> InternalCompletableFuture<E> asyncInvoke(Operation operation) {
        try {
            OperationService operationService = getNodeEngine().getOperationService();
            //noinspection unchecked
            return (InternalCompletableFuture<E>) operationService.invokeOnPartition(
                    AtomicLongService.SERVICE_NAME, operation, partitionId);
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }
}
