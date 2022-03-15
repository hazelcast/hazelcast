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

package com.hazelcast.internal.monitor;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.json.internal.JsonSerializable;
import com.hazelcast.version.MemberVersion;
import com.hazelcast.version.Version;

/**
 * Local node statistics to be used by {@link MemberState} implementations.
 */
public interface NodeState extends JsonSerializable {

    /**
     * @return the current node state (status)
     */
    ClusterState getClusterState();

    /**
     * @return the current node state (status)
     */
    com.hazelcast.instance.impl.NodeState getNodeState();

    /**
     * @return the current version of the Cluster
     */
    Version getClusterVersion();

    /**
     * @return the codebase version of the Node
     */
    MemberVersion getMemberVersion();

}
