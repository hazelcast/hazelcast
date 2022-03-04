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

package com.hazelcast.internal.dynamicconfig;

import com.hazelcast.internal.config.ConfigDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;

public class DynamicConfigPreJoinOperation
        extends AbstractDynamicConfigOperation {

    private IdentifiedDataSerializable[] configs;
    private ConfigCheckMode configCheckMode;

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public DynamicConfigPreJoinOperation(IdentifiedDataSerializable[] configs, ConfigCheckMode configCheckMode) {
        this.configs = configs;
        this.configCheckMode = configCheckMode;
    }

    public DynamicConfigPreJoinOperation() {

    }

    @Override
    public void run() throws Exception {
        ClusterWideConfigurationService service = getService();
        for (IdentifiedDataSerializable config : configs) {
            service.registerConfigLocally(config, configCheckMode);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeInt(configs.length);
        for (IdentifiedDataSerializable config: configs) {
            out.writeObject(config);
        }
        out.writeString(configCheckMode.name());
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        configs = new IdentifiedDataSerializable[size];
        for (int i = 0; i < size; i++) {
            configs[i] = in.readObject();
        }
        configCheckMode = ConfigCheckMode.valueOf(in.readString());
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.DYNAMIC_CONFIG_PRE_JOIN_OP;
    }
}
