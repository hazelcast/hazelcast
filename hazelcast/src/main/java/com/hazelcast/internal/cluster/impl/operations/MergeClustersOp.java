/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.cluster.impl.operations;

import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.auditlog.AuditlogTypeIds;
import com.hazelcast.cluster.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;

import static com.hazelcast.internal.cluster.impl.ClusterServiceImpl.SPLIT_BRAIN_HANDLER_EXECUTOR_NAME;

public class MergeClustersOp extends AbstractClusterOperation {

    private Address newTargetAddress;

    public MergeClustersOp() {
    }

    public MergeClustersOp(Address newTargetAddress) {
        this.newTargetAddress = newTargetAddress;
    }

    @Override
    public void run() {
        final Address caller = getCallerAddress();
        final NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        final Node node = nodeEngine.getNode();
        final ClusterServiceImpl clusterService = node.getClusterService();
        final Address masterAddress = clusterService.getMasterAddress();
        final ILogger logger = node.loggingService.getLogger(this.getClass().getName());

        boolean local = caller == null;
        if (!local && !caller.equals(masterAddress)) {
            logger.warning("Ignoring merge instruction sent from non-master endpoint: " + caller);
            return;
        }

        logger.warning(node.getThisAddress() + " is merging to " + newTargetAddress
                + ", because: instructed by master " + masterAddress);
        node.getNodeExtension().getAuditlogService().eventBuilder(AuditlogTypeIds.CLUSTER_MERGE)
            .message("Merging this cluster into another one")
            .addParameter("masterAddress", masterAddress)
            .addParameter("targetAddress", newTargetAddress)
            .log();
        nodeEngine.getExecutionService().execute(SPLIT_BRAIN_HANDLER_EXECUTOR_NAME, () -> clusterService.merge(newTargetAddress));
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        newTargetAddress = in.readObject();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeObject(newTargetAddress);
    }

    @Override
    public int getClassId() {
        return ClusterDataSerializerHook.MERGE_CLUSTERS;
    }
}
