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
import com.hazelcast.cluster.impl.JoinMessage;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;

public class JoinCheckOperation extends AbstractOperation implements JoinOperation {

    private JoinMessage request;
    private JoinMessage response;

    private transient boolean removeCaller;

    public JoinCheckOperation() {
    }

    public JoinCheckOperation(JoinMessage request) {
        this.request = request;
    }

    @Override
    public void run() {
        ClusterServiceImpl service = getService();
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        Node node = nodeEngine.getNode();

        if (!preCheck(node)) {
            return;
        }

        if (!masterCheck(node)) {
            return;
        }

        if (request != null) {
            ILogger logger = getLogger();
            try {
                if (service.validateJoinMessage(request)) {
                    response = node.createSplitBrainJoinMessage();
                }
                if (logger.isFinestEnabled()) {
                    logger.finest("Returning " + response + " to " + getCallerAddress());
                }
            } catch (Exception e) {
                if (logger.isFinestEnabled()) {
                    logger.finest("Could not validate split-brain join message! -> " + e.getMessage());
                }
            }
        }
    }

    private boolean masterCheck(Node node) {
        ILogger logger = getLogger();
        Address caller = getCallerAddress();
        ClusterServiceImpl service = getService();

        if (node.isMaster()) {
            if (service.getMember(caller) != null) {
                logger.info("Removing " + caller + ", since it thinks it's already split from this cluster "
                        + "and looking to merge.");
                removeCaller = true;
            }
            return true;
        } else {
            // ping master to check if it's still valid
            service.sendMasterConfirmation();
            logger.info("Ignoring join check from " + caller
                    + ", because this node is not master...");
            return false;
        }
    }

    @Override
    public void afterRun() throws Exception {
        if (removeCaller) {
            ClusterServiceImpl service = getService();
            Address caller = getCallerAddress();
            service.removeAddress(caller);
        }
    }

    private boolean preCheck(Node node) {
        ILogger logger = getLogger();
        if (!node.joined()) {
            logger.info("Ignoring join check from " + getCallerAddress()
                    + ", because this node is not joined to a cluster yet...");
            return false;
        }

        if (!node.isActive()) {
            logger.info("Ignoring join check from " + getCallerAddress() + ", because this node is not active...");
            return false;
        }
        return true;
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    protected void readInternal(final ObjectDataInput in) throws IOException {
        request = new JoinMessage();
        request.readData(in);
    }

    @Override
    protected void writeInternal(final ObjectDataOutput out) throws IOException {
        request.writeData(out);
    }
}

