/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nullable;

public class RemoveDynamicConfigOperation extends UpdateDynamicConfigOperation {
    public RemoveDynamicConfigOperation() {
    }

    public RemoveDynamicConfigOperation(IdentifiedDataSerializable config, int memberListVersion, @Nullable String namespace) {
        super(config, memberListVersion, namespace);
    }

    @Override
    public void run() throws Exception {
        ClusterWideConfigurationService service = getService();
        service.deregisterConfigLocally(config);
        super.run();
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.REMOVE_DYNAMIC_CONFIG_OP;
    }
}
