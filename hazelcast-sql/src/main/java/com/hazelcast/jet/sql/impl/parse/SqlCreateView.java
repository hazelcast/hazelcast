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
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.List;

import static com.hazelcast.jet.sql.impl.parse.ParserResource.RESOURCE;
import static com.hazelcast.jet.sql.impl.validate.ValidationUtil.isCatalogObjectNameValid;

/**
 * AST node representing a CREATE VIEW statement.
 *
 * @since 5.1
 */
public class SqlCreateView extends SqlCreate {
    private static final SqlOperator CREATE_VIEW = new HazelcastCreateViewOperator();

    private final SqlIdentifier name;
    private SqlNode query;

    public SqlCreateView(SqlParserPos pos, boolean replace, boolean ifNotExists, SqlIdentifier name, SqlNode query) {
        super(CREATE_VIEW, pos, replace, ifNotExists);
        this.name = name;
        this.query = query;
    }

    public String name() {
        return name.toString();
    }

    public SqlNode getQuery() {
        return query;
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
        if (ifNotExists) {
            writer.keyword("IF NOT EXISTS");
        }
        name.unparse(writer, leftPrec, rightPrec);
        writer.keyword("AS");
        writer.newlineAndIndent();
        query.unparse(writer, 0, 0);
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        if (getReplace() && ifNotExists) {
            throw validator.newValidationError(this, RESOURCE.orReplaceWithIfNotExistsNotSupported());
        }

        if (!isCatalogObjectNameValid(name)) {
            throw validator.newValidationError(name, RESOURCE.viewIncorrectSchema());
        }

        query = validator.validate(query);
    }
}
