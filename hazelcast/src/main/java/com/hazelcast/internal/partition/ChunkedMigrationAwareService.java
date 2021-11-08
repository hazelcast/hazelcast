/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.map.impl.ChunkSupplier;
import com.hazelcast.map.impl.ChunkSuppliers;

import java.util.Collection;

/**
 * Data migration can be done lazily, by using this interface's functionality.
 */
public interface ChunkedMigrationAwareService
        extends FragmentedMigrationAwareService {

    default ChunkSupplier newChunkSupplier(PartitionReplicationEvent event,
                                           Collection<ServiceNamespace> namespaces) {
        return ChunkSuppliers.newSingleChunkSupplier(getClass().getSimpleName(),
                () -> prepareReplicationOperation(event, namespaces));
    }
}
