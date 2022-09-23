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

import com.hazelcast.config.PermissionConfig;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.management.ManagementDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Propagates {@link PermissionConfig} changes to members.
 */
public class UpdatePermissionConfigOperation extends AbstractManagementOperation {

    private Set<PermissionConfig> permissionConfigs;

    public UpdatePermissionConfigOperation() {
    }

    @SuppressWarnings("unused")
    public UpdatePermissionConfigOperation(Set<PermissionConfig> permissionConfigs) {
        this.permissionConfigs = permissionConfigs;
    }

    @Override
    public int getClassId() {
        return ManagementDataSerializerHook.UPDATE_PERMISSION_CONFIG_OPERATION;
    }

    @Override
    public void run() throws Exception {
        Node node = ((NodeEngineImpl) getNodeEngine()).getNode();
        try {
            node.securityContext.refreshPermissions(permissionConfigs);
        } catch (IllegalStateException e) {
            throw new RetryableHazelcastException("Permission refresh was not allowed at this time", e);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(permissionConfigs.size());
        for (PermissionConfig permissionConfig : permissionConfigs) {
            permissionConfig.writeData(out);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int configSize = in.readInt();
        permissionConfigs = new HashSet<>(configSize);
        for (int i = 0; i < configSize; i++) {
            PermissionConfig permissionConfig = new PermissionConfig();
            permissionConfig.readData(in);
            permissionConfigs.add(permissionConfig);
        }
    }
}
