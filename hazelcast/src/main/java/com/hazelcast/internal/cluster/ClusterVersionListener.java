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

package com.hazelcast.internal.cluster;

import com.hazelcast.cluster.Cluster;
import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.version.Version;

/**
 * Listeners interested in acting upon cluster version update should implement this interface.
 * Services registered with the Hazelcast {@link com.hazelcast.spi.impl.servicemanager.ServiceManager} which implement this
 * interface do not have have to register themselves, as their {@link #onClusterVersionChange(Version)} method will be
 * invoked automatically.
 *
 * Other listeners have to register themselves with {@link NodeExtension#registerListener(Object)}.
 * Upon registration, the listener's {@link #onClusterVersionChange(Version)} method will be invoked once with the current
 * value of the cluster version.
 *
 * @see Cluster#getClusterVersion()
 * @see ClusterService#changeClusterVersion(Version)
 * @since 3.8
 */
public interface ClusterVersionListener {

    /**
     * Invoked on registered listeners after the new cluster version has been applied. Listeners are executed synchronously
     * immediately after {@code ClusterStateManager#version} has been updated and while the cluster service lock
     * {@code ClusterServiceImpl#lock} is still locked, as part of the commit phase of the transaction changing
     * the cluster version. Unhandled exceptions from listeners implementation will break the new version commit and a slow
     * implementation will stall the system and may cause a transaction timeout.
     * If new cluster version is {@code null} and property
     * {@link ClusterProperty#INIT_CLUSTER_VERSION} is set, the version set by this property
     * will be provided as argument to the listener. If neither are set, running node's codebase version will be used.
     *
     * @param newVersion the new version
     */
    void onClusterVersionChange(Version newVersion);

}
