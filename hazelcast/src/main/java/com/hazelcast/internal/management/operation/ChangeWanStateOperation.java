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

package com.hazelcast.internal.management.operation;

import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.AbstractLocalOperation;
import com.hazelcast.wan.WanPublisherState;
import com.hazelcast.wan.impl.WanReplicationService;

/**
 * Stop, pause or resume WAN replication for the given {@code wanReplicationName} and {@code wanPublisherId}.
 */
public class ChangeWanStateOperation extends AbstractLocalOperation {

    private String wanReplicationName;
    private String wanPublisherId;
    private WanPublisherState state;

    public ChangeWanStateOperation(String wanReplicationName,
                                   String wanPublisherId,
                                   WanPublisherState state) {
        this.wanReplicationName = wanReplicationName;
        this.wanPublisherId = wanPublisherId;
        this.state = state;
    }

    @Override
    public void run() throws Exception {
        NodeEngine nodeEngine = getNodeEngine();
        WanReplicationService wanReplicationService = nodeEngine.getWanReplicationService();

        switch (state) {
            case REPLICATING:
                wanReplicationService.resume(wanReplicationName, wanPublisherId);
                break;
            case PAUSED:
                wanReplicationService.pause(wanReplicationName, wanPublisherId);
                break;
            case STOPPED:
                wanReplicationService.stop(wanReplicationName, wanPublisherId);
                break;
            default:
                break;
        }
    }
}
