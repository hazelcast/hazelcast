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

package com.hazelcast.internal.jmx;

import com.hazelcast.internal.monitor.LocalWanPublisherStats;
import com.hazelcast.internal.monitor.LocalWanStats;
import com.hazelcast.wan.impl.WanReplicationService;

import java.util.Map;

/**
 * Management bean for a single WAN replication publisher. Since OS and EE
 * WAN publishers do not share a single common interface, we need to access
 * them through the WAN replication service until such an interface is introduced.
 */
@ManagedDescription("WanPublisher")
public class WanPublisherMBean extends HazelcastMBean<WanReplicationService> {
    private final String wanReplicationName;
    private final String wanPublisherId;

    public WanPublisherMBean(WanReplicationService wanReplicationService,
                             String wanReplicationName,
                             String wanPublisherId,
                             ManagementService service) {
        super(wanReplicationService, service);
        this.wanReplicationName = wanReplicationName;
        this.wanPublisherId = wanPublisherId;
        this.objectName = service.createObjectName("WanPublisher",
                wanReplicationName + "." + wanPublisherId);
    }

    @ManagedAnnotation("state")
    @ManagedDescription("State of the WAN replication publisher")
    public String getState() {
        final Map<String, LocalWanStats> wanStats = managedObject.getStats();
        if (wanStats == null) {
            return "";
        }
        final LocalWanStats wanReplicationStats = wanStats.get(wanReplicationName);
        final Map<String, LocalWanPublisherStats> wanDelegatingPublisherStats = wanReplicationStats.getLocalWanPublisherStats();
        final LocalWanPublisherStats wanPublisherStats = wanDelegatingPublisherStats.get(wanPublisherId);
        return wanPublisherStats.getPublisherState().name();
    }

}
