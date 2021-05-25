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

package com.hazelcast.jet.sql.impl.connector.map.journal;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.extract.JournalQueryTarget;
import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.sql.impl.extract.QueryTarget;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.optimizer.PlanObjectKey;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.TableStatistics;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.Nonnull;
import java.util.List;

public class IMapJournalTable extends JetTable {

    private final QueryTargetDescriptor keyQueryTargetDescriptor;
    private final QueryTargetDescriptor valueQueryTargetDescriptor;

    private final SupplierEx<QueryTarget> queryTargetSupplier;

    public IMapJournalTable(
            @Nonnull SqlConnector sqlConnector,
            @Nonnull List<TableField> fields,
            @Nonnull String schemaName,
            @Nonnull String name,
            @Nonnull TableStatistics statistics,
            @Nonnull QueryTargetDescriptor keyQueryDescriptor,
            @Nonnull QueryTargetDescriptor valueQueryTargetDescriptor
    ) {
        super(sqlConnector, fields, schemaName, name, statistics);
        this.keyQueryTargetDescriptor = keyQueryDescriptor;
        this.valueQueryTargetDescriptor = valueQueryTargetDescriptor;
        this.queryTargetSupplier = () -> new JournalQueryTarget(keyQueryTargetDescriptor, valueQueryTargetDescriptor);
    }

    @Override
    public PlanObjectKey getObjectKey() {
        return null;
    }

    String[] paths() {
        return getFields().stream().map(field -> field.getName()).toArray(String[]::new);
    }

    QueryDataType[] types() {
        return getFields().stream().map(TableField::getType).toArray(QueryDataType[]::new);
    }

    public QueryTargetDescriptor getValueQueryDescriptor() {
        return valueQueryTargetDescriptor;
    }

    public QueryTargetDescriptor getKeyQueryDescriptor() {
        return keyQueryTargetDescriptor;
    }

    public static SupplierEx<QueryTarget>
        getQueryTargetSupplier(
                @Nonnull QueryTargetDescriptor keyQueryTargetDescriptor,
                @Nonnull QueryTargetDescriptor valueQueryTargetDescriptor
    ) {
        return () -> new JournalQueryTarget(keyQueryTargetDescriptor, valueQueryTargetDescriptor);
    }
}
