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

package com.hazelcast.internal.management.operation;

import com.hazelcast.internal.dynamicconfig.ConfigurationService;
import com.hazelcast.internal.management.ManagementDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.UUID;

public class UpdateConfigOperation
        extends AbstractDynamicConfigOperation {

    private String configPatch;

    public UpdateConfigOperation() {
    }

    public UpdateConfigOperation(String configPatch) {
        this.configPatch = configPatch;
    }

    @Override
    public int getClassId() {
        return ManagementDataSerializerHook.UPDATE_CONFIG_OPERATION;
    }

    @Override
    protected UUID startUpdateProcess() {
        ConfigurationService configService = getService();
        return configService.updateAsync(configPatch);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        out.writeString(configPatch);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        configPatch = in.readString();
    }
}
