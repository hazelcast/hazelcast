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

package com.hazelcast.concurrent.countdownlatch.operations;

import com.hazelcast.concurrent.countdownlatch.LatchKey;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.spi.impl.AbstractNamedOperation;

import static com.hazelcast.concurrent.countdownlatch.CountDownLatchDataSerializerHook.F_ID;
import static com.hazelcast.concurrent.countdownlatch.CountDownLatchService.SERVICE_NAME;

abstract class AbstractCountDownLatchOperation extends AbstractNamedOperation
        implements PartitionAwareOperation, IdentifiedDataSerializable {

    AbstractCountDownLatchOperation() {
    }

    AbstractCountDownLatchOperation(String name) {
        super(name);
    }

    @Override
    public final String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public final int getFactoryId() {
        return F_ID;
    }

    WaitNotifyKey waitNotifyKey() {
        return new LatchKey(name);
    }
}
