/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.connection.nio;

import java.util.concurrent.Future;

/**
 * selects a connection to listen cluster state(membership and partition listener),
 * Keeps those listeners available when connection disconnected by picking a new connection.
 * Changing cluster is also handled in this class(Blue/green feature)
 */
public interface ClusterConnectorService {

    void connectToCluster();

    Future<Void> connectToClusterAsync();

    boolean mainConnectionExists();

    /**
     * @return client connection that is used to listen cluster at the moment
     */
    ClientConnection getClusterConnection();

    void shutdown();

}
