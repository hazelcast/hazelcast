/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.management;

import com.hazelcast.cluster.ClusterState;

public class MCClusterMetadata {
    private ClusterState currentState;
    private long clusterTime;
    private String memberVersion;
    private String jetVersion;

    public ClusterState getCurrentState() {
        return currentState;
    }

    public void setCurrentState(ClusterState currentState) {
        this.currentState = currentState;
    }

    public long getClusterTime() {
        return clusterTime;
    }

    public void setClusterTime(long clusterTime) {
        this.clusterTime = clusterTime;
    }

    public String getMemberVersion() {
        return memberVersion;
    }

    public void setMemberVersion(String memberVersion) {
        this.memberVersion = memberVersion;
    }

    public String getJetVersion() {
        return jetVersion;
    }

    public void setJetVersion(String jetVersion) {
        this.jetVersion = jetVersion;
    }
}
