/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.application;


import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.jet.api.hazelcast.JetService;
import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;

public class ServerApplicationInvocation<T> extends AbstractApplicationInvocation<Operation, T> {
    private final JetService jetService;
    private final NodeEngine nodeEngine;

    public ServerApplicationInvocation(Operation operation, Address address,
                                       JetService jetService, NodeEngine nodeEngine) {
        super(operation, address);

        this.jetService = jetService;
        this.nodeEngine = nodeEngine;
    }

    @SuppressWarnings("unchecked")
    protected T execute(Operation operation, Address address) {
        ClusterService cs = this.nodeEngine.getClusterService();
        OperationService os = this.nodeEngine.getOperationService();
        boolean returnsResponse = operation.returnsResponse();

        try {
            if (cs.getThisAddress().equals(address)) {
                // Locally we can call the operation directly
                operation.setNodeEngine(this.nodeEngine);
                operation.setCallerUuid(this.nodeEngine.getLocalMember().getUuid());
                operation.setService(this.jetService);
                operation.run();
            } else {
                if (returnsResponse) {
                    InvocationBuilder ib = os.createInvocationBuilder(JetService.SERVICE_NAME, operation, address);
                    ib.invoke().get();
                } else {
                    os.send(operation, address);
                }
            }
        } catch (Throwable e) {
            throw JetUtil.reThrow(e);
        }

        return (T) operation.getResponse();
    }
}
