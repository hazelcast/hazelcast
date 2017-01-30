/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.wan;

import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.instance.Node;

/**
 * Implementations of this interface represent a replication endpoint, normally another
 * Hazelcast cluster only reachable over a wide area network.
 */
public interface WanReplicationEndpoint
        extends WanReplicationPublisher {

    /**
     * Initializes the endpoint using the given arguments.
     *
     * @param node      the current node that tries to connect
     * @param wanReplicationConfig {@link WanReplicationConfig} instance
     * @param publisherConfig  {@link WanPublisherConfig} instance
     */
    void init(Node node, WanReplicationConfig wanReplicationConfig, WanPublisherConfig publisherConfig);

    /**
     * Closes the endpoint and its internal connections and shuts down other internal states
     */
    void shutdown();
}
