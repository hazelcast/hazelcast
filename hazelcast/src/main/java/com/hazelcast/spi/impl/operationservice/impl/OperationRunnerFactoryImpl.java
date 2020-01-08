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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.spi.impl.operationexecutor.OperationRunner;
import com.hazelcast.spi.impl.operationexecutor.OperationRunnerFactory;

import static com.hazelcast.spi.impl.operationservice.Operation.GENERIC_PARTITION_ID;
import static com.hazelcast.spi.impl.operationservice.impl.OperationRunnerImpl.AD_HOC_PARTITION_ID;

class OperationRunnerFactoryImpl implements OperationRunnerFactory {
    private OperationServiceImpl operationService;
    private int genericId;

    OperationRunnerFactoryImpl(OperationServiceImpl operationService) {
        this.operationService = operationService;
    }

    @Override
    public OperationRunner createAdHocRunner() {
        return new OperationRunnerImpl(operationService, AD_HOC_PARTITION_ID, 0, null);
    }

    @Override
    public OperationRunner createPartitionRunner(int partitionId) {
        return new OperationRunnerImpl(operationService, partitionId, 0, operationService.failedBackupsCount);
    }

    @Override
    public OperationRunner createGenericRunner() {
        return new OperationRunnerImpl(operationService, GENERIC_PARTITION_ID, genericId++, null);
    }
}
