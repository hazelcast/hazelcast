/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.schema;

import com.hazelcast.jet.sql.impl.connector.virtual.ViewTable;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastJsonType;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlNameMatchers;

import java.util.List;

/**
 * Custom catalog reader that allows for setting predefined schema paths and wrapping of returned tables.
 */
public class HazelcastCalciteCatalogReader extends CalciteCatalogReader {

    public HazelcastCalciteCatalogReader(
            CalciteSchema rootSchema,
            List<List<String>> schemaPaths,
            RelDataTypeFactory typeFactory,
            CalciteConnectionConfig config
    ) {
        // Call the protected constructor that is not visible otherwise.
        super(
                rootSchema,
                SqlNameMatchers.withCaseSensitive(config != null && config.caseSensitive()),
                schemaPaths,
                typeFactory,
                config
        );
    }

    /**
     * Hook into the Apache Calcite table creation process and wrap the Calcite table into our own implementation
     * that resolves signatures of tables with pushed-down projects and filters properly.
     */
    @Override
    public Prepare.PreparingTable getTable(List<String> names) {
        // Resolve the original table as usual.
        Prepare.PreparingTable table = super.getTable(names);

        if (table == null) {
            return null;
        }

        HazelcastTable hzTable = table.unwrap(HazelcastTable.class);
        assert hzTable != null;
        if (hzTable.getTarget() instanceof ViewTable) {
            return new HazelcastViewRelOptTable(table, ((ViewTable) hzTable.getTarget()).getViewRel());
        }

        // Wrap it into our own table.
        return new HazelcastRelOptTable(table);
    }

    @Override
    public RelDataType getNamedType(SqlIdentifier typeName) {
        if (HazelcastTypeUtils.isObjectIdentifier(typeName)) {
            return typeFactory.createSqlType(SqlTypeName.ANY);
        }

        if (HazelcastTypeUtils.isJsonIdentifier(typeName)) {
            return HazelcastJsonType.create(true);
        }

        return super.getNamedType(typeName);
    }
}
