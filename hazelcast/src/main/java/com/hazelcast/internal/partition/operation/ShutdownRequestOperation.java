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

package com.hazelcast.internal.partition.operation;

import com.hazelcast.cluster.Member;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.MigrationCycleOperation;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.logging.ILogger;
import com.hazelcast.cluster.Address;
import com.hazelcast.spi.impl.NodeEngine;

public class ShutdownRequestOperation extends AbstractPartitionOperation implements MigrationCycleOperation {

    public ShutdownRequestOperation() {
    }

    @Override
    public void run() {
        InternalPartitionServiceImpl partitionService = getService();
        final ILogger logger = getLogger();
        final Address caller = getCallerAddress();

        final NodeEngine nodeEngine = getNodeEngine();
        final ClusterService clusterService = nodeEngine.getClusterService();

        if (clusterService.isMaster()) {
            Member member = clusterService.getMember(caller);
            if (member != null) {
                if (logger.isFinestEnabled()) {
                    logger.finest("Received shutdown request from " + caller);
                }
                partitionService.onShutdownRequest(member);
            } else {
                logger.warning("Ignoring shutdown request from " + caller + " because it is not a member");
            }
        } else {
            logger.warning("Received shutdown request from " + caller + " but this node is not master.");
        }
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public String getServiceName() {
        return InternalPartitionService.SERVICE_NAME;
    }

    @Override
    public int getClassId() {
        return PartitionDataSerializerHook.SHUTDOWN_REQUEST;
    }
}
