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

package com.hazelcast.sql.impl.calcite;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.validate.SqlNameMatcher;

import java.util.Collections;
import java.util.List;

// TODO: Do we need it still?
// TODO: RelDataTypeDrillImpl
public class HazelcastCalciteCatalogReader extends CalciteCatalogReader {

    public HazelcastCalciteCatalogReader(CalciteSchema rootSchema, RelDataTypeFactory typeFactory,
        CalciteConnectionConfig config) {
        super(rootSchema, Collections.emptyList(), typeFactory, config);
    }

    @Override
    public Prepare.PreparingTable getTable(List<String> names) {
        // TODO
        return super.getTable(names);
    }

    @Override
    public void lookupOperatorOverloads(SqlIdentifier opName, SqlFunctionCategory category, SqlSyntax syntax,
        List<SqlOperator> operatorList, SqlNameMatcher nameMatcher) {
        super.lookupOperatorOverloads(opName, category, syntax, operatorList, nameMatcher);
    }
}
