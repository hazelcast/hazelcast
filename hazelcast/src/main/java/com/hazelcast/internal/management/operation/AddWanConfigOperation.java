/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.management.ManagementDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.wan.WanReplicationService;

import java.io.IOException;

/**
 * Operation to add a new {@link WanReplicationConfig} at runtime.
 * <p>
 * NOTE: needs to be {@link Versioned} since some classes in the
 * WanReplicationConfig hierarchy are Versioned. Unfortunately, this class
 * has until now serialized by invoking
 * {@link ObjectDataOutput#writeData(Data)} directly, which circumvents
 */
public class AddWanConfigOperation extends AbstractManagementOperation implements Versioned {

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
        // RU_COMPAT_3_11
        if (out.getVersion().isGreaterOrEqual(Versions.V3_12)) {
            // using this method is nicer since the object
            // can implement Versioned and have a version injected
            out.writeObject(wanReplicationConfig);
        } else {
            wanReplicationConfig.writeData(out);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        // RU_COMPAT_3_11
        if (in.getVersion().isGreaterOrEqual(Versions.V3_12)) {
            wanReplicationConfig = in.readObject();
        } else {
            wanReplicationConfig = new WanReplicationConfig();
            wanReplicationConfig.readData(in);
        }
    }

    @Override
    public String getServiceName() {
        return WanReplicationService.SERVICE_NAME;
    }

    @Override
    public int getId() {
        return ManagementDataSerializerHook.ADD_WAN_CONFIG;
    }
}
