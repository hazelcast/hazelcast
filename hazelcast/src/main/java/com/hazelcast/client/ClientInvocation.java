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

package com.hazelcast.client;

import com.hazelcast.nio.Address;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @mdogan 4/25/13
 */
final class ClientInvocation {

    private final NodeEngine nodeEngine;
    private final Operation op;
    private final String serviceName;

    private final Address target;

    private final int partitionId;
    private final int replicaIndex;

    private final ClientEndpoint endpoint;

    ClientInvocation(NodeEngine nodeEngine, Operation op, String serviceName, int partitionId, int replicaIndex, ClientEndpoint endpoint) {
        this.nodeEngine = nodeEngine;
        this.op = op;
        this.serviceName = serviceName;
        this.partitionId = partitionId;
        this.replicaIndex = replicaIndex;
        this.endpoint = endpoint;
        target = null;
    }

    ClientInvocation(NodeEngine nodeEngine, Operation operation, String serviceName, Address target, ClientEndpoint endpoint) {
        this.nodeEngine = nodeEngine;
        this.op = operation;
        this.serviceName = serviceName;
        this.target = target;
        this.endpoint = endpoint;
        partitionId = -1;
        replicaIndex = 0;
    }

    public Object invoke() throws InterruptedException, ExecutionException, TimeoutException {
        op.setCallerUuid(endpoint.getUuid());
        final InvocationBuilder builder;
        if (target == null) {
            builder = nodeEngine.getOperationService().createInvocationBuilder(serviceName, op, partitionId)
                    .setReplicaIndex(replicaIndex);
        } else {
            builder = nodeEngine.getOperationService().createInvocationBuilder(serviceName, op, target);
        }
        builder.setTryCount(100).setCallTimeout(20 * 1000);
        Invocation inv = builder.build();
        Future f = inv.invoke();
        return f.get(30, TimeUnit.SECONDS);
    }


}
