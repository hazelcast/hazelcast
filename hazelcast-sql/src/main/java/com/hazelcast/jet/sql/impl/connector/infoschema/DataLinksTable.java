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
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.datalink.DataLinkCatalogEntry;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static java.util.Arrays.asList;

public class DataLinksTable extends InfoSchemaTable {
    private static final String NAME = "datalinks";

    private static final List<TableField> FIELDS = asList(
            new TableField("datalink_catalog", QueryDataType.VARCHAR, false),
            new TableField("datalink_schema", QueryDataType.VARCHAR, false),
            new TableField("datalink_name", QueryDataType.VARCHAR, false),
            new TableField("datalink_type", QueryDataType.VARCHAR, false),
            new TableField("datalink_shared", QueryDataType.BOOLEAN, false),
            new TableField("datalink_options", QueryDataType.VARCHAR, false),
            new TableField("datalink_source", QueryDataType.VARCHAR, false)
    );

    private final String dataLinkSchema;
    private final Collection<DataLinkCatalogEntry> dataLinkCatalogEntries;

    public DataLinksTable(String catalog,
                          String schemaName,
                          String dataLinkSchema,
                          Collection<DataLinkCatalogEntry> dataLinkCatalogEntries) {
        super(FIELDS, catalog, schemaName, NAME, new ConstantTableStatistics(0));
        this.dataLinkSchema = dataLinkSchema;
        this.dataLinkCatalogEntries = dataLinkCatalogEntries;
    }

    @Override
    protected List<Object[]> rows() {
        List<Object[]> rows = new ArrayList<>(dataLinkCatalogEntries.size());
        for (DataLinkCatalogEntry dl : dataLinkCatalogEntries) {
            Object[] row = new Object[]{
                    catalog(),
                    dataLinkSchema,
                    dl.name(),
                    dl.type(),
                    dl.isShared(),
                    uncheckCall(() -> JsonUtil.toJson(dl.options())),
                    dl.source().name()
            };
            rows.add(row);
        }
        return rows;
    }
}
