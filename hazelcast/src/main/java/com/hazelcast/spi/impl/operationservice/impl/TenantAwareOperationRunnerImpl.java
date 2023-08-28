/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.util.LatencyDistribution;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.util.concurrent.ConcurrentMap;

public class TenantAwareOperationRunnerImpl extends OperationRunnerImpl {

    TenantAwareOperationRunnerImpl(OperationServiceImpl operationService,
                                   int partitionId,
                                   int genericId,
                                   Counter failedBackupsCounter,
                                   ConcurrentMap<Class, LatencyDistribution> opLatencyDistributions) {
        super(operationService, partitionId, genericId, failedBackupsCounter, opLatencyDistributions);
    }

    @Override
    protected void run(Operation op, long startNanos) {
        try {
            executedOperationsCounter.inc();

            boolean publishCurrentTask = publishCurrentTask();
            if (publishCurrentTask) {
                currentTask = op;
            }

            try {
                if (!metWithPreconditions(op)) {
                    return;
                }

                if (op.isTenantAvailable()) {
                    op.pushThreadContext();
                    op.beforeRun();
                    call(op);
                } else {
                    operationService.operationExecutor.execute(op);
                }
            } catch (Throwable e) {
                handleOperationError(op, e);
            } finally {
                op.afterRunFinal();
                if (publishCurrentTask) {
                    currentTask = null;
                }
                op.popThreadContext();
                record(op, startNanos);
            }
        } finally {
            op.clearThreadContext();
        }
    }
}
