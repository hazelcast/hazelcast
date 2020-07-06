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

package com.hazelcast.sql.impl.exec.scan;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.List;

/**
 * Executor for map scan.
 */
public class MapScanExec extends AbstractMapScanExec {

    protected final MapContainer map;
    protected final PartitionIdSet partitions;

    public MapScanExec(
        int id,
        MapContainer map,
        PartitionIdSet partitions,
        QueryTargetDescriptor keyDescriptor,
        QueryTargetDescriptor valueDescriptor,
        List<QueryPath> fieldPaths,
        List<QueryDataType> fieldTypes,
        List<Integer> projects,
        Expression<Boolean> filter,
        InternalSerializationService serializationService
    ) {
        super(id, map.getName(), keyDescriptor, valueDescriptor, fieldPaths, fieldTypes, projects, filter, serializationService);

        this.map = map;
        this.partitions = partitions;
    }

    @Override
    protected Extractors createExtractors() {
        return MapScanExecUtils.createExtractors(map);
    }

    @Override
    protected int getMigrationStamp() {
        return map.getMapServiceContext().getService().getMigrationStamp();
    }

    @Override
    protected boolean validateMigrationStamp(int migrationStamp) {
        return map.getMapServiceContext().getService().validateMigrationStamp(migrationStamp);
    }

    @Override
    protected KeyValueIterator createIterator() {
        return MapScanExecUtils.createIterator(map, partitions);
    }

    @Override
    protected boolean isDestroyed() {
        return map.isDestroyed();
    }

    public MapContainer getMap() {
        return map;
    }

    public PartitionIdSet getPartitions() {
        return partitions;
    }
}
