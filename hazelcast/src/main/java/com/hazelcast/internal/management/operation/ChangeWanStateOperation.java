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

package com.hazelcast.internal.management.operation;

import com.hazelcast.config.WanPublisherState;
import com.hazelcast.spi.AbstractLocalOperation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.wan.WanReplicationService;

/**
 * Stop, pause or resume WAN replication for the given {@code wanReplicationName} and {@code targetGroupName}.
 */
public class ChangeWanStateOperation extends AbstractLocalOperation {

    private String schemeName;
    private String publisherName;
    private WanPublisherState state;

    public ChangeWanStateOperation(String schemeName, String publisherName, WanPublisherState state) {
        this.schemeName = schemeName;
        this.publisherName = publisherName;
        this.state = state;
    }

    @Override
    public void run() throws Exception {
        NodeEngine nodeEngine = getNodeEngine();
        WanReplicationService wanReplicationService = nodeEngine.getWanReplicationService();

        switch (state) {
            case REPLICATING:
                wanReplicationService.resume(schemeName, publisherName);
                break;
            case PAUSED:
                wanReplicationService.pause(schemeName, publisherName);
                break;
            case STOPPED:
                wanReplicationService.stop(schemeName, publisherName);
                break;
            default:
                break;
        }
    }
}
