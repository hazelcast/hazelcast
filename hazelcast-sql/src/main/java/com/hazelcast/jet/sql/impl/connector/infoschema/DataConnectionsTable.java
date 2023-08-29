/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.connector.infoschema;

import com.hazelcast.jet.json.JsonUtil;
import com.hazelcast.jet.sql.impl.connector.SqlConnectorCache;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.dataconnection.DataConnectionCatalogEntry;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static java.util.Arrays.asList;

public class DataConnectionsTable extends InfoSchemaTable {
    private static final String NAME = "dataconnections";

    private static final List<TableField> FIELDS = asList(
            new TableField("catalog", QueryDataType.VARCHAR, false),
            new TableField("schema", QueryDataType.VARCHAR, false),
            new TableField("name", QueryDataType.VARCHAR, false),
            new TableField("type", QueryDataType.VARCHAR, false),
            new TableField("shared", QueryDataType.BOOLEAN, false),
            new TableField("options", QueryDataType.VARCHAR, false),
            new TableField("source", QueryDataType.VARCHAR, false)
    );

    private final String dataConnectionSchema;
    private final Collection<DataConnectionCatalogEntry> dataConnectionCatalogEntries;
    private final SqlConnectorCache connectorCache;
    private final boolean securityEnabled;

    public DataConnectionsTable(String catalog,
                                String schemaName,
                                String dataConnectionSchema,
                                Collection<DataConnectionCatalogEntry> dataConnectionCatalogEntries,
                                SqlConnectorCache connectorCache,
                                boolean securityEnabled) {
        super(FIELDS, catalog, schemaName, NAME, new ConstantTableStatistics(0));
        this.dataConnectionSchema = dataConnectionSchema;
        this.dataConnectionCatalogEntries = dataConnectionCatalogEntries;
        this.connectorCache = connectorCache;
        this.securityEnabled = securityEnabled;
    }

    @Override
    protected List<Object[]> rows() {
        List<Object[]> rows = new ArrayList<>(dataConnectionCatalogEntries.size());
        for (DataConnectionCatalogEntry dl : dataConnectionCatalogEntries) {
            final Map<String, String> options;
            if (!securityEnabled) {
                options = dl.options();
            } else {
                options = new TreeMap<>();
                Set<String> safeOptions = connectorCache.forType(dl.type()).nonSensitiveConnectorOptions();
                for (Map.Entry<String, String> e : dl.options().entrySet()) {
                    if (safeOptions.contains(e.getKey())) {
                        options.put(e.getKey(), e.getValue());
                    }
                }
            }
            Object[] row = new Object[]{
                    catalog(),
                    dataConnectionSchema,
                    dl.name(),
                    dl.type(),
                    dl.isShared(),
                    uncheckCall(() -> JsonUtil.toJson(options)),
                    dl.source().name()
            };
            rows.add(row);
        }
        return rows;
    }
}
