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

package com.hazelcast.internal.management.operation;

import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.internal.management.ManagementDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.wan.WanReplicationService;

import java.io.IOException;

/**
 * Operation to add a new {@link WanReplicationConfig} at runtime.
 */
public class AddWanConfigOperation extends AbstractManagementOperation {

    private WanReplicationConfig wanReplicationConfig;

    @SuppressWarnings("unused")
    public AddWanConfigOperation() {
    }

    public AddWanConfigOperation(WanReplicationConfig wanReplicationConfig) {
        this.wanReplicationConfig = wanReplicationConfig;
    }

    @Override
    public void run() throws Exception {
        WanReplicationService wanReplicationService = getNodeEngine().getWanReplicationService();
        wanReplicationService.addWanReplicationConfig(wanReplicationConfig);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        wanReplicationConfig.writeData(out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        wanReplicationConfig = new WanReplicationConfig();
        wanReplicationConfig.readData(in);
    }

    @Override
    public int getId() {
        return ManagementDataSerializerHook.ADD_WAN_CONFIG;
    }
}
