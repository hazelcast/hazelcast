/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.impl.NodeEngineImpl;

public class GroupMismatchOp extends AbstractClusterOperation {

    public GroupMismatchOp() {
    }

    @Override
    public void run() {
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        Connection connection = getConnection();

        String message = "Node could not join cluster at node: " + connection.getEndPoint()
                + " Cause: the target cluster has a different group-name";

        connection.close(message, null);

        ILogger logger = nodeEngine.getLogger("com.hazelcast.cluster");
        logger.warning(message);

        Node node = nodeEngine.getNode();
        node.getJoiner().blacklist(getCallerAddress(), true);
    }

    @Override
    public int getId() {
        return ClusterDataSerializerHook.GROUP_MISMATCH;
    }
}

