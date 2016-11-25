/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.monitor;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.internal.management.JsonSerializable;
import com.hazelcast.version.ClusterVersion;
import com.hazelcast.version.MemberVersion;

public interface NodeState extends JsonSerializable {

    /**
     * @return the current node state (status)
     */
    ClusterState getClusterState();

    /**
     * @return the current node state (status)
     */
    com.hazelcast.instance.NodeState getNodeState();

    /**
     * @return the current version of the Cluster
     */
    ClusterVersion getClusterVersion();

    /**
     * @return the codebase version of the Node
     */
    MemberVersion getMemberVersion();

    /**
     * @return true if rolling upgrade is enabled
     */
    boolean isRollingUpgradeEnabled();

}
