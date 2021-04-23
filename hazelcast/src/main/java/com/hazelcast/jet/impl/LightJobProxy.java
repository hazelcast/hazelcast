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

package com.hazelcast.jet.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.jet.LightJob;
import com.hazelcast.jet.impl.operation.CancelLightJobOperation;
import com.hazelcast.spi.impl.NodeEngine;

import java.util.concurrent.Future;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

public class LightJobProxy implements LightJob {

    private final NodeEngine nodeEngine;
    private final long jobId;
    private final Address coordinatorAddress;
    private final Future<Void> future;

    LightJobProxy(NodeEngine nodeEngine, long jobId, Address coordinatorAddress, Future<Void> future) {
        this.nodeEngine = nodeEngine;
        this.jobId = jobId;
        this.coordinatorAddress = coordinatorAddress;
        this.future = future;
    }

    @Override
    public void join() {
        try {
            future.get();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    public void cancel() {
        CancelLightJobOperation operation = new CancelLightJobOperation(jobId);
        nodeEngine.getOperationService()
                .createInvocationBuilder(JetService.SERVICE_NAME, operation, coordinatorAddress)
                .invoke()
                .join();
    }
}
