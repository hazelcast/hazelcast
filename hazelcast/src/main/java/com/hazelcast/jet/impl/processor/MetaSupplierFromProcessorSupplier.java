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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.serialization.SerializableByConvention;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.security.PermissionsUtil;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.security.Permission;
import java.util.List;
import java.util.function.Function;

@SerializableByConvention
public class MetaSupplierFromProcessorSupplier implements ProcessorMetaSupplier, DataSerializable {
    private int preferredLocalParallelism;
    private ProcessorSupplier processorSupplier;
    private Permission permission;

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
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(preferredLocalParallelism);
        out.writeObject(processorSupplier);
        out.writeObject(permission);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        preferredLocalParallelism = in.readInt();
        processorSupplier = in.readObject();
        permission = in.readObject();
    }

    @Override
    public Permission getRequiredPermission() {
        return permission;
    }
}
