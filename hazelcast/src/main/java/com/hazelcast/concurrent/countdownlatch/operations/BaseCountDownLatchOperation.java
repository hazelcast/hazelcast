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

package com.hazelcast.concurrent.countdownlatch.operations;

import com.hazelcast.concurrent.countdownlatch.CountDownLatchService;
import com.hazelcast.concurrent.countdownlatch.LatchKey;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.spi.impl.AbstractNamedOperation;

abstract class BaseCountDownLatchOperation extends AbstractNamedOperation
        implements PartitionAwareOperation {

    protected BaseCountDownLatchOperation() {
    }

    protected BaseCountDownLatchOperation(String name) {
        super(name);
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public final String getServiceName() {
        return CountDownLatchService.SERVICE_NAME;
    }

    protected WaitNotifyKey waitNotifyKey() {
        return new LatchKey(name);
    }
}
