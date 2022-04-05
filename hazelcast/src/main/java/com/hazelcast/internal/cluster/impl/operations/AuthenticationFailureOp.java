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
import com.hazelcast.spi.impl.NodeEngineImpl;

public class AuthenticationFailureOp extends AbstractClusterOperation {

    public AuthenticationFailureOp() {
    }

    @Override
    public void run() {
        final NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        final Node node = nodeEngine.getNode();
        final ILogger logger = nodeEngine.getLogger("com.hazelcast.security");
        logger.severe("Node could not join cluster. Authentication failed on master node! Node is going to shutdown now!");
        node.shutdown(true);
    }

    @Override
    public int getClassId() {
        return ClusterDataSerializerHook.AUTH_FAILURE;
    }
}
