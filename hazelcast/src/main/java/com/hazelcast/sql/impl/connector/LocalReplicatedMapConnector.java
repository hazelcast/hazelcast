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

package com.hazelcast.sql.impl.connector;

import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.ExternalTable.ExternalField;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.schema.map.ReplicatedMapTableResolver;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.stream.Collectors.toList;

// TODO: do we want to keep it? maps are auto discovered...
public class LocalReplicatedMapConnector extends SqlKeyValueConnector {

    public static final String TYPE_NAME = "com.hazelcast.LocalReplicatedMap";

    @Override
    public String typeName() {
        return TYPE_NAME;
    }

    @Nonnull @Override
    public Table createTable(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull String schemaName,
            @Nonnull String tableName,
            @Nonnull List<ExternalField> externalFields,
            @Nonnull Map<String, String> options
    ) {
        ReplicatedMapService mapService = nodeEngine.getService(ReplicatedMapService.SERVICE_NAME);
        return Objects.requireNonNull(
                ReplicatedMapTableResolver.createTable(nodeEngine, mapService, schemaName, tableName,
                        toMapFields(externalFields), options));
    }

    private static List<TableField> toMapFields(List<ExternalField> externalFields) {
        return externalFields.stream()
                             .map(field -> new MapTableField(field.name(), field.type(), QueryPath.create(field.name())))
                             .collect(toList());
    }
}
