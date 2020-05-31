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

import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.ExternalTable.ExternalField;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.schema.map.MapTableUtils;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.stream.Collectors.toList;

// TODO: do we want to keep it? maps are auto discovered...
public class LocalPartitionedMapConnector extends SqlKeyValueConnector {

    public static final String TYPE_NAME = "com.hazelcast.LocalPartitionedMap";

    @Override
    public String typeName() {
        return TYPE_NAME;
    }

    @Nonnull @Override
    public Table createTable(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull String schemaName,
            @Nonnull String name,
            @Nonnull List<ExternalField> externalFields,
            @Nonnull Map<String, String> options
    ) {
        String objectName = options.getOrDefault(TO_OBJECT_NAME, name);

        return Objects.requireNonNull(
            createTable0(nodeEngine, schemaName, objectName, toMapTableFields(externalFields), options));
    }

    private static PartitionedMapTable createTable0(
        NodeEngine nodeEngine,
        String schemaName,
        String mapName,
        List<TableField> fields,
        Map<String, String> options
    ) {
        MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);
        MapServiceContext context = mapService.getMapServiceContext();

        long estimatedRowCount = MapTableUtils.estimatePartitionedMapRowCount(nodeEngine, context, mapName);

        // TODO: VO: As soon as custom fields are introduced, it is not clear how to handle indexes, distribution field and
        //  descriptors propperly. The method "toMapTableFields" doesn't handle key/value distinction properly, therefore we
        //  loose information necessary for field extraction.
        return new PartitionedMapTable(
            schemaName,
            mapName,
            fields,
            new ConstantTableStatistics(estimatedRowCount),
            GenericQueryTargetDescriptor.INSTANCE,
            GenericQueryTargetDescriptor.INSTANCE,
            Collections.emptyList(),
            PartitionedMapTable.DISTRIBUTION_FIELD_ORDINAL_NONE,
            options
        );
    }

    private static List<TableField> toMapTableFields(List<ExternalField> externalFields) {
        return externalFields.stream()
                             .map(field -> new MapTableField(field.name(), field.type(), QueryPath.create(field.name())))
                             .collect(toList());
    }
}
