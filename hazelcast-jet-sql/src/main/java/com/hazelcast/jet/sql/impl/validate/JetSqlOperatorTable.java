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

package com.hazelcast.jet.sql.impl.validate;

import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlNameMatchers;

import java.util.List;

public final class JetSqlOperatorTable extends ReflectiveSqlOperatorTable {

    private static final JetSqlOperatorTable INSTANCE = new JetSqlOperatorTable();

    static {
        INSTANCE.init();
    }

    private JetSqlOperatorTable() {
    }

    static JetSqlOperatorTable instance() {
        return INSTANCE;
    }

    @Override
    public void lookupOperatorOverloads(
            SqlIdentifier name,
            SqlFunctionCategory category,
            SqlSyntax syntax,
            List<SqlOperator> operators,
            SqlNameMatcher nameMatcher
    ) {
        super.lookupOperatorOverloads(name, category, syntax, operators, SqlNameMatchers.withCaseSensitive(false));
    }
}
