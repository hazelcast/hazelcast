/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.serialization.SerializableByConvention;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.connector.ConnectorNameAware;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.security.PermissionsUtil;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.security.Permission;
import java.util.List;
import java.util.function.Function;

@SerializableByConvention
public class MetaSupplierFromProcessorSupplier implements ProcessorMetaSupplier, DataSerializable, ConnectorNameAware {
    private int preferredLocalParallelism;
    private ProcessorSupplier processorSupplier;
    private Permission permission;
    @Nullable
    private String connectorName;

    // for deserialization
    @SuppressWarnings("unused")
    public MetaSupplierFromProcessorSupplier() {
    }

    public MetaSupplierFromProcessorSupplier(
            int preferredLocalParallelism,
            Permission permission,
            ProcessorSupplier processorSupplier
    ) {
        this.preferredLocalParallelism = preferredLocalParallelism;
        this.permission = permission;
        this.processorSupplier = processorSupplier;
    }

    public MetaSupplierFromProcessorSupplier(
            int preferredLocalParallelism,
            Permission permission,
            ProcessorSupplier processorSupplier,
            @Nullable String connectorName
    ) {
        this.preferredLocalParallelism = preferredLocalParallelism;
        this.permission = permission;
        this.processorSupplier = processorSupplier;
        this.connectorName = connectorName;
    }

    @Override
    public void init(@Nonnull Context context) throws Exception {
        PermissionsUtil.checkPermission(processorSupplier, context);
    }

    @Override
    public int preferredLocalParallelism() {
        return preferredLocalParallelism;
    }

    @Nonnull @Override
    public Function<? super Address, ? extends ProcessorSupplier> get(@Nonnull List<Address> addresses) {
        return address -> processorSupplier;
    }

    @Override
    public Permission getRequiredPermission() {
        return permission;
    }

    @Override
    public boolean isReusable() {
        return true;
    }

    @Override
    public boolean initIsCooperative() {
        return true;
    }

    @Override
    public boolean closeIsCooperative() {
        return true;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(preferredLocalParallelism);
        out.writeObject(processorSupplier);
        out.writeObject(permission);
        out.writeString(connectorName);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        preferredLocalParallelism = in.readInt();
        processorSupplier = in.readObject();
        permission = in.readObject();
        connectorName = in.readString();
    }

    @Nullable
    @Override
    public String getConnectorName() {
        return connectorName;
    }
}
