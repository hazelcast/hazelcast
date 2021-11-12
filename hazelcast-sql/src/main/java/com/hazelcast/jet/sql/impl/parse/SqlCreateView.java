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

package com.hazelcast.jet.sql.impl.parse;

import com.google.common.collect.ImmutableList;
import com.hazelcast.jet.sql.impl.validate.operators.special.HazelcastCreateViewOperator;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

import static com.hazelcast.jet.sql.impl.schema.InformationSchemaCatalog.SCHEMA_NAME_PUBLIC;
import static com.hazelcast.sql.impl.QueryUtils.CATALOG;

public class SqlCreateView extends SqlCreate {
    private static final SqlOperator CREATE_VIEW = new HazelcastCreateViewOperator();

    private final SqlIdentifier name;
    private SqlNode query;


    public SqlCreateView(SqlParserPos pos, boolean replace, SqlIdentifier name, SqlNode query) {
        super(CREATE_VIEW, pos, replace, false);
        this.name = name;
        this.query = query;
    }

    public String name() {
        return name.toString();
    }

    public SqlNode getQuery() {
        return query;
    }

    public void setQuery(SqlNode query) {
        this.query = query;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(name, query);
    }

    @Override
    public SqlOperator getOperator() {
        return CREATE_VIEW;
    }

    /**
     * Copied from {@link org.apache.calcite.sql.ddl.SqlCreateView}
     */
    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (getReplace()) {
            writer.keyword("CREATE OR REPLACE");
        } else {
            writer.keyword("CREATE");
        }
        writer.keyword("VIEW");
        name.unparse(writer, leftPrec, rightPrec);
        writer.keyword("AS");
        writer.newlineAndIndent();
        query.unparse(writer, 0, 0);
    }

    /**
     * Returns true if the view name is in a valid schema,
     * that is it must be either:
     * <ul>
     *     <li>a simple name
     *     <li>a name in schema "public"
     *     <li>a name in schema "hazelcast.public"
     * </ul>
     */
    @SuppressWarnings({"checkstyle:BooleanExpressionComplexity", "BooleanMethodIsAlwaysInverted"})
    static boolean isViewNameValid(SqlIdentifier name) {
        return name.names.size() == 1
                || name.names.size() == 2 && SCHEMA_NAME_PUBLIC.equals(name.names.get(0))
                || name.names.size() == 3 && CATALOG.equals(name.names.get(0))
                && SCHEMA_NAME_PUBLIC.equals(name.names.get(1));
    }
}
