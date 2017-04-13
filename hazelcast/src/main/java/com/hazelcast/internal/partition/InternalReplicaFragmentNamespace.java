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

package com.hazelcast.internal.partition;

import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.ReplicaFragmentNamespace;

import java.io.IOException;

/**
 * Internal {@link ReplicaFragmentNamespace} implementation used by partitioning system to identity
 * non-fragmented service structures. All partition replica data belonging to service those do not implement
 * {@link com.hazelcast.spi.FragmentedMigrationAwareService} will be registered with {@code InternalReplicaFragmentNamespace}.
 *
 * @see com.hazelcast.spi.FragmentedMigrationAwareService
 * @since 3.9
 */
public final class InternalReplicaFragmentNamespace implements ReplicaFragmentNamespace, IdentifiedDataSerializable {

    public static final ReplicaFragmentNamespace INSTANCE = new InternalReplicaFragmentNamespace();

    public InternalReplicaFragmentNamespace() {
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
    public void writeData(ObjectDataOutput out) throws IOException {
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
    }

    @Override
    public String toString() {
        return "InternalReplicaFragmentNamespace";
    }

    @Override
    public int getFactoryId() {
        return PartitionDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return PartitionDataSerializerHook.INTERNAL_FRAGMENT_NAMESPACE;
    }
}
