/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cluster;

import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.impl.NodeServiceImpl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.logging.Level;

public class MergeClustersOperation extends AbstractClusterOperation {

    private Address newTargetAddress = null;

    public MergeClustersOperation() {
    }

    public MergeClustersOperation(Address newTargetAddress) {
        super();
        this.newTargetAddress = newTargetAddress;
    }

    public void run() {
        final Address endpoint = getCaller();
        final NodeServiceImpl nodeService = (NodeServiceImpl) getNodeService();
        final Node node = nodeService.getNode();
        final Address masterAddress = node.getMasterAddress();
        final ILogger logger = node.loggingService.getLogger(this.getClass().getName());
        if (endpoint == null || !endpoint.equals(masterAddress)) {
            logger.log(Level.WARNING, "Merge instruction sent from non-master endpoint: " + endpoint);
            return;
        }
        logger.log(Level.WARNING, node.getThisAddress() + " is merging to " + newTargetAddress
                + ", because: instructed by master " + masterAddress);
        node.getJoiner().setTargetAddress(newTargetAddress);
        node.hazelcastInstance.getLifecycleService().restart();
    }

    public void readInternal(DataInput in) throws IOException {
        super.readInternal(in);
        newTargetAddress = new Address();
        newTargetAddress.readData(in);
    }

    public void writeInternal(DataOutput out) throws IOException {
        super.writeInternal(out);
        newTargetAddress.writeData(out);
    }
}
