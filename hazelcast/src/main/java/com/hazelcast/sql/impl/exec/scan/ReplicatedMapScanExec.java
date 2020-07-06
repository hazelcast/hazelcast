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
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.replicatedmap.impl.ReplicatedMapProxy;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.List;

/**
 * Executor for map scan.
 */
@SuppressWarnings("rawtypes")
public class ReplicatedMapScanExec extends AbstractMapScanExec {

    private final ReplicatedMapProxy map;

    public ReplicatedMapScanExec(
        int id,
        ReplicatedMapProxy map,
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
    }

    @Override
    protected int getMigrationStamp() {
        // TODO: Not implemented yet
        return 0;
    }

    @Override
    protected boolean validateMigrationStamp(int migrationStamp) {
        // TODO: Not implemented yet
        return true;
    }

    @Override
    protected KeyValueIterator createIterator() {
        return new ReplicatedMapScanExecIterator(map);
    }

    @Override
    protected boolean isDestroyed() {
        // TODO: Not implemented yet
        return false;
    }

    @Override
    protected Extractors createExtractors() {
        return map.getExtractors();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{mapName=" + mapName + ", fieldPaths=" + fieldPaths
            + ", projects=" + projects + ", filter=" + filter + '}';
    }
}
