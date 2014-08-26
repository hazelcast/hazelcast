/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.impl.NodeEngineImpl;

public class MasterClaimOperation extends AbstractOperation implements JoinOperation {

    private transient boolean approvedAsMaster = false;

    @Override
    public void run() {
        final NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        Node node = nodeEngine.getNode();
        Joiner joiner = node.getJoiner();
        final ILogger logger = node.getLogger(getClass().getName());
        if (joiner instanceof TcpIpJoiner) {
            TcpIpJoiner tcpIpJoiner = (TcpIpJoiner) joiner;
            final Address endpoint = getCallerAddress();
            final Address masterAddress = node.getMasterAddress();
            approvedAsMaster = !tcpIpJoiner.isClaimingMaster() && !node.isMaster()
                    && (masterAddress == null || masterAddress.equals(endpoint));
        } else {
            approvedAsMaster = false;
            logger.warning("This node requires MulticastJoin strategy!");
        }
        if (logger.isFinestEnabled()) {
            logger.finest("Sending '" + approvedAsMaster + "' for master claim of node: " + getCallerAddress());
        }
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return approvedAsMaster;
    }
}
