/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.management.operation;

import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.wan.WanReplicationService;

/**
 * Enables/Disable publishing events to target cluster in WAN Replication {@link com.hazelcast.wan.WanReplicationService}
 * on a node. This operation does not block adding new events to event queue.
 */
public class ChangeWanStateOperation extends AbstractOperation {

    private String schemeName;
    private String publisherName;
    private boolean start;

    public ChangeWanStateOperation() {
    }

    public ChangeWanStateOperation(String schemeName, String publisherName, boolean start) {
        this.schemeName = schemeName;
        this.publisherName = publisherName;
        this.start = start;
    }

    @Override
    public void run() throws Exception {
        NodeEngine nodeEngine = getNodeEngine();
        WanReplicationService wanReplicationService = nodeEngine.getWanReplicationService();

        if (start) {
            wanReplicationService.resume(schemeName, publisherName);
        } else {
            wanReplicationService.pause(schemeName, publisherName);
        }
    }
}
