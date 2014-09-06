/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.wan.impl;

import com.hazelcast.wan.ReplicationEventObject;
import com.hazelcast.wan.WanReplicationEndpoint;
import com.hazelcast.wan.WanReplicationPublisher;

/**
 * Delegating replication publisher implementation
 */
final class WanReplicationPublisherDelegate
        implements WanReplicationPublisher {

    final String name;
    final WanReplicationEndpoint[] endpoints;

    public WanReplicationPublisherDelegate(String name, WanReplicationEndpoint[] endpoints) {
        this.name = name;
        this.endpoints = endpoints;
    }

    public WanReplicationEndpoint[] getEndpoints() {
        return endpoints;
    }

    public String getName() {
        return name;
    }

    public void publishReplicationEvent(String serviceName, ReplicationEventObject eventObject) {
        for (WanReplicationEndpoint endpoint : endpoints) {
            endpoint.publishReplicationEvent(serviceName, eventObject);
        }
    }
}
