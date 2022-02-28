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

package com.hazelcast.internal.partition;

import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.internal.services.ServiceNamespace;

/**
 * Internal {@link ServiceNamespace} implementation used by partitioning system to identify
 * non-fragmented service structures. All partition replica data belonging to service which do not implement
 * {@link FragmentedMigrationAwareService} will be registered with
 * {@code NonFragmentedServiceNamespace}.
 *
 * @see FragmentedMigrationAwareService
 * @since 3.9
 */
public final class NonFragmentedServiceNamespace implements ServiceNamespace, IdentifiedDataSerializable {

    public static final NonFragmentedServiceNamespace INSTANCE = new NonFragmentedServiceNamespace();

    private NonFragmentedServiceNamespace() {
    }

    @Override
    public String getServiceName() {
        return "hz:default-replica-namespace";
    }

    @Override
    public boolean equals(Object o) {
        return this == o || !(o == null || getClass() != o.getClass());
    }

    @Override
    public int hashCode() {
        return getServiceName().hashCode();
    }

    @Override
    public void writeData(ObjectDataOutput out) {
    }

    @Override
    public void readData(ObjectDataInput in) {
    }

    @Override
    public String toString() {
        return "NonFragmentedServiceNamespace";
    }

    @Override
    public int getFactoryId() {
        return PartitionDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return PartitionDataSerializerHook.NON_FRAGMENTED_SERVICE_NAMESPACE;
    }
}
