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
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;

/**
 * When a node wants to join the cluster, its sends its ConfigCheck to the cluster where it is validated.
 * If the ConfigCheck fails, this operation is send to that node to trigger him to shutdown himself. This
 * way he will not join the cluster.
 *
 * @see AuthenticationFailureOp
 */
public class ConfigMismatchOp extends AbstractClusterOperation {

    private String msg;

    public ConfigMismatchOp() {
    }

    public ConfigMismatchOp(String msg) {
        this.msg = msg;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeString(msg);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        msg = in.readString();
    }

    @Override
    public void run() {
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        Node node = nodeEngine.getNode();
        ILogger logger = nodeEngine.getLogger("com.hazelcast.cluster");
        logger.severe("Node could not join cluster. A Configuration mismatch was detected: "
                + msg + " Node is going to shutdown now!");
        node.shutdown(true);
    }

    @Override
    public int getClassId() {
        return ClusterDataSerializerHook.CONFIG_MISMATCH;
    }
}

