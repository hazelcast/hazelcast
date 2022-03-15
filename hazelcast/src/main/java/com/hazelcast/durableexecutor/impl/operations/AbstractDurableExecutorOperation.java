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

package com.hazelcast.durableexecutor.impl.operations;

import com.hazelcast.durableexecutor.impl.DistributedDurableExecutorService;
import com.hazelcast.durableexecutor.impl.DurableExecutorContainer;
import com.hazelcast.durableexecutor.impl.DurableExecutorDataSerializerHook;
import com.hazelcast.durableexecutor.impl.DurableExecutorPartitionContainer;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.AbstractNamedOperation;

abstract class AbstractDurableExecutorOperation extends AbstractNamedOperation implements IdentifiedDataSerializable {

    private transient DurableExecutorContainer executorContainer;

    AbstractDurableExecutorOperation() {
    }

    AbstractDurableExecutorOperation(String name) {
        super(name);
    }

    public String getServiceName() {
        return DistributedDurableExecutorService.SERVICE_NAME;
    }

    public DurableExecutorContainer getExecutorContainer() {
        if (executorContainer == null) {
            DistributedDurableExecutorService service = getService();
            DurableExecutorPartitionContainer partitionContainer = service.getPartitionContainer(getPartitionId());
            executorContainer = partitionContainer.getOrCreateContainer(name);
        }
        return executorContainer;
    }

    public int getSyncBackupCount() {
        return executorContainer.getDurability();
    }

    public int getAsyncBackupCount() {
        return 0;
    }

    public boolean shouldBackup() {
        return true;
    }

    @Override
    public int getFactoryId() {
        return DurableExecutorDataSerializerHook.F_ID;
    }
}
