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

package com.hazelcast.spi.impl.eventservice.impl.operations;

import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.eventservice.impl.Registration;

import java.util.UUID;
import java.util.function.Supplier;

/**
 * Supplier that creates {@link DeregistrationOperation}s for a listener registration or topic.
 */
public class DeregistrationOperationSupplier implements Supplier<Operation> {
    private final String serviceName;
    private final String topic;
    private final UUID id;
    private final int orderKey;
    private final ClusterService clusterService;

    public DeregistrationOperationSupplier(Registration reg, ClusterService clusterService) {
        serviceName = reg.getServiceName();
        topic = reg.getTopic();
        id = reg.getId();
        orderKey = -1;
        this.clusterService = clusterService;
    }

    public DeregistrationOperationSupplier(String serviceName, String topic, int orderKey, ClusterService clusterService) {
        this.serviceName = serviceName;
        this.topic = topic;
        id = null;
        this.orderKey = orderKey;
        this.clusterService = clusterService;
    }

    @Override
    public Operation get() {
        return new DeregistrationOperation(topic, id, orderKey, clusterService.getMemberListVersion())
                .setServiceName(serviceName);
    }
}
