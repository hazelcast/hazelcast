/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.tenantcontrol.impl;

import com.hazelcast.internal.util.MapUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.SpiDataSerializerHook;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.tenantcontrol.TenantControl;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

/**
 * Operation which exchanges tenant control between members.
 * Can be used to exchange a single or multiple tenant controls each belonging
 * to a different distributed object.
 *
 * @since 4.1
 */
public class AppendTenantControlOperation extends Operation
        implements IdentifiedDataSerializable, MutatingOperation {

    private ConcurrentMap<String, ConcurrentMap<String, TenantControl>> tenantControlMap;

    private String distributedObjectServiceName;
    private String distributedObjectName;
    private TenantControl tenantControl;

    public AppendTenantControlOperation() {
    }

    public AppendTenantControlOperation(@Nonnull String distributedObjectServiceName,
                                        @Nonnull String distributedObjectName,
                                        @Nonnull TenantControl tenantControl) {
        this.distributedObjectServiceName = distributedObjectServiceName;
        this.distributedObjectName = distributedObjectName;
        this.tenantControl = tenantControl;
    }

    public AppendTenantControlOperation(
            @Nonnull ConcurrentMap<String, ConcurrentMap<String, TenantControl>> tenantControlMap) {
        this.tenantControlMap = tenantControlMap;
    }

    @Override
    public void run() {
        TenantControlServiceImpl service = getNodeEngine().getTenantControlService();
        tenantControlMap.forEach((serviceName, objectMap) ->
                objectMap.forEach((objectName, tenantControl) ->
                        service.appendTenantControl(serviceName, objectName, tenantControl)));
    }

    @Override
    public Object getResponse() {
        return true;
    }

    @Override
    public void readInternal(ObjectDataInput in) throws IOException {
        int serviceCount = in.readInt();

        tenantControlMap = MapUtil.createConcurrentHashMap(serviceCount);
        for (int i = 0; i < serviceCount; i++) {
            String serviceName = in.readUTF();
            int objectCount = in.readInt();

            ConcurrentMap<String, TenantControl> objectMap = MapUtil.createConcurrentHashMap(objectCount);
            tenantControlMap.put(serviceName, objectMap);

            for (int j = 0; j < objectCount; j++) {
                objectMap.put(in.readUTF(), in.readObject());
            }
        }

        distributedObjectServiceName = in.readUTF();
        tenantControl = in.readObject();
    }

    @Override
    public void writeInternal(ObjectDataOutput out) throws IOException {
        if (tenantControlMap != null) {
            // we are sending multiple tenant controls for
            // different services and objects
            out.writeObject(tenantControlMap);
            out.writeInt(tenantControlMap.size());
            for (Entry<String, ConcurrentMap<String, TenantControl>> serviceEntry : tenantControlMap.entrySet()) {
                String serviceName = serviceEntry.getKey();
                ConcurrentMap<String, TenantControl> tenantControlPerObject = serviceEntry.getValue();

                out.writeUTF(serviceName);
                out.writeInt(tenantControlPerObject.size());
                for (Entry<String, TenantControl> objectEntry : tenantControlPerObject.entrySet()) {
                    out.writeUTF(objectEntry.getKey());
                    out.writeObject(objectEntry.getValue());
                }
            }
        } else {
            // we are sending just one tenant control
            out.writeInt(1);
            out.writeUTF(distributedObjectServiceName);
            out.writeInt(1);
            out.writeObject(distributedObjectName);
            out.writeObject(tenantControl);
        }
    }

    @Override
    public int getFactoryId() {
        return SpiDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SpiDataSerializerHook.APPEND_TENANT_CONTROL_OPERATION;
    }
}
