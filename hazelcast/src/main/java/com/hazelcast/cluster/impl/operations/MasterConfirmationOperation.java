/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cluster.impl.operations;

import com.hazelcast.cluster.impl.ClusterServiceImpl;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;

import java.io.IOException;

public class MasterConfirmationOperation extends AbstractClusterOperation implements AllowedDuringPassiveState {

    private long timestamp;

    public MasterConfirmationOperation() {
    }

    public MasterConfirmationOperation(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public void run() {
        final Address endpoint = getCallerAddress();
        if (endpoint == null) {
            return;
        }

        final ClusterServiceImpl clusterService = getService();
        final ILogger logger = getLogger();
        final MemberImpl member = clusterService.getMember(endpoint);
        if (member == null) {
            logger.warning("MasterConfirmation has been received from " + endpoint
                    + ", but it is not a member of this cluster!");
            OperationService operationService = getNodeEngine().getOperationService();
            operationService.send(new MemberRemoveOperation(clusterService.getThisAddress()), endpoint);
        } else {
            if (clusterService.isMaster()) {
                clusterService.getClusterHeartbeatManager().acceptMasterConfirmation(member, timestamp);
            } else {
                logger.warning(endpoint + " has sent MasterConfirmation, but this node is not master!");
            }
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(timestamp);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        timestamp = in.readLong();
    }
}
