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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.instance.Node;
import com.hazelcast.util.Preconditions;
import com.hazelcast.version.Version;

//
// Just a PoC to have the upgrade logic encompasses somewhere for now...
//
public final class ClusterVersionService {

    private ClusterVersionService() {
    }

    public static void validateJoinRequestOnMaster(Node currentNode, JoinRequest joinRequest) {
        // continue until cluster state consistent during check
        while (true) {
            Version clusterVersion = currentNode.getClusterService().getClusterVersion();
            Version joinerVersion = joinRequest.getVersion();

            doValidateJoinRequestOnMaster(clusterVersion, joinerVersion);

            Version clusterVersionNow = currentNode.getClusterService().getClusterVersion();
            if (clusterVersion.equals(clusterVersionNow)) {
                return;
            }
        }
    }

    private static void doValidateJoinRequestOnMaster(Version clusterVersion, Version joinerVersion) {

        // major version check
        if (clusterVersion.getMajor() != joinerVersion.getMajor()) {
            throw new ConfigMismatchException("Illegal joiner version. Cluster=[" + clusterVersion + "] vs. Joiner=["
                    + joinerVersion + "]");
        }

        // here we have equal major version

        if (clusterVersion.getMinor() < joinerVersion.getMinor()) {
            // cluster operating in a lower-version mode -> we allow a higher version node to join
            return;
        }

        if (clusterVersion.getMinor() == joinerVersion.getMinor()) {
            // if cluster is NOT operating in a lower-version mode -> that's fine, a node with the same version is joining

            // if cluster is operating in a lower-version mode -> we still allow a lower-version node to join
            // this is particularly useful if a rolling-upgrade has failed and there's a roll-back, so older nodes are joining
            // in order to rollback the node-count of "older" nodes to the desired state.
            return;
        }

        if (clusterVersion.getMinor() > joinerVersion.getMinor()) {
            // we disallow joining of node with lower version than the cluster version
            throw new ConfigMismatchException("Illegal joiner version. Cluster=[" + clusterVersion + "] vs. Joiner=["
                    + joinerVersion + "]");
        }
    }

    public static void validateClusterVersionChange(Version version) {
        if (version.getPatch() > 0) {
            throw new IllegalArgumentException("Could not set cluster's patch version to " + version.getPatch()
                    + " Just set the major & minor versions, e.g. '3.8' or '3.9'");
        }
    }

    public static void validateNodeVersionCompatibility(Node currentNode, Version version) {
        Preconditions.checkNotNull(version);
        Version nodeVersion = currentNode.getVersion();

        // node can either work at its codebase version (native mode)
        // or at a  minor version that's smaller by one (emulated mode)
        if (nodeVersion.getMajor() == version.getMajor()
                && (nodeVersion.getMinor() == version.getMinor() || nodeVersion.getMinor() == version.getMinor() + 1)) {
            return;
        }

        throw new IllegalArgumentException("Node's codebase version " + nodeVersion
                + " is incompatible with the requested version " + version);
    }


}
