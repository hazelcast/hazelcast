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

package com.hazelcast.sql.impl.calcite.schema;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.RelDataTypeFactory;
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

        // Wrap it into our own table.
        return new HazelcastRelOptTable(table);
    }
}
