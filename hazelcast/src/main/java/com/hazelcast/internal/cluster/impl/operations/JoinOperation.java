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

package com.hazelcast.internal.cluster.impl.operations;

import com.hazelcast.instance.impl.Node;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.impl.operationservice.UrgentSystemOperation;

/**
 * Marker interface for join and post-join operations.
 */
public interface JoinOperation extends UrgentSystemOperation, AllowedDuringPassiveState, IdentifiedDataSerializable {

    /**
     * Join operations are not authenticated. This means that if any join
     * operation includes node shutdown, a third party can just send that
     * operation to any member and shutdown the node. To prevent this we only
     * allow shutdown if the node isn't joined to the cluster. Furthermore, we
     * don't allow shutdown if a member is joined before. This is to prevent
     * vulnerabilities during split brain.
     *
     * @param node   node to shutdown
     * @param reason reason of the shutdown request
     * @throws IllegalStateException if the node is joined before
     * @see ShutdownNodeOp
     */
    static void verifyCanShutdown(Node node, String reason) {
        if (node.getClusterService().isJoined()) {
            throw new IllegalStateException(
                    "Ignoring termination request as the node is already joined to a cluster. "
                            + "Termination request reason: " + reason);
        } else if (node.getClusterService().isJoinedBefore()) {
            throw new IllegalStateException(
                    " Ignoring termination request as the node has been joined to a cluster before. "
                            + "Termination request reason:" + reason);
        }
    }
}
