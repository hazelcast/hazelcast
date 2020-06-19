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

import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.schema.ExternalTable.ExternalField;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTableResolver;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

public class LocalPartitionedMapConnector extends LocalAbstractMapConnector {

    // TODO rename to LocalIMap
    public static final String TYPE_NAME = "com.hazelcast.LocalPartitionedMap";

    @Override
    public String typeName() {
        return TYPE_NAME;
    }

    @Nonnull @Override
    public Table createTable(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull String schemaName,
            @Nonnull String tableName,
            @Nonnull Map<String, String> options,
            @Nullable List<ExternalField> externalFields
    ) {
        String objectName = options.getOrDefault(TO_OBJECT_NAME, tableName);

        PartitionedMapTable res = PartitionedMapTableResolver.createTable(nodeEngine, schemaName, objectName,
                options, toMapFields(externalFields), true);
        assert res != null && res.getException() == null : res;
        return res;
    }
}
