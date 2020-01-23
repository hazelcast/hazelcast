/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.log.impl.operations;

import com.hazelcast.log.impl.LogContainer;
import com.hazelcast.log.impl.LogDataSerializerHook;
import com.hazelcast.log.impl.LogService;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.NamedOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;

public abstract class LogOperation extends Operation implements NamedOperation, PartitionAwareOperation, IdentifiedDataSerializable {

    String name;

    public LogOperation() {
    }

    public LogOperation(String name) {
        this.name = name;
    }


    public LogContainer getContainer() {
        LogService service = getService();
        LogContainer container = service.getContainer(getPartitionId(), name);
        container.touch();
        return container;
    }

    @Override
    public String getServiceName() {
        return LogService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return LogDataSerializerHook.F_ID;
    }

    @Override
    public String getName() {
        return name;
    }
}
