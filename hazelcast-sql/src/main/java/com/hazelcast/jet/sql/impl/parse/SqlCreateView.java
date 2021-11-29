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
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.sql.impl.parse.ParserResource.RESOURCE;
import static com.hazelcast.jet.sql.impl.validate.ValidationUtil.isCatalogObjectNameValid;

/**
 * TODO: doc
 */
public class SqlCreateView extends SqlCreate {
    private static final SqlOperator CREATE_VIEW = new HazelcastCreateViewOperator();

    private final SqlIdentifier name;
    private SqlNode query;
    private final List<SqlSelect> innerSelects = new ArrayList<>();

    public SqlCreateView(SqlParserPos pos, boolean replace, boolean ifNotExists, SqlIdentifier name, SqlNode query) {
        super(CREATE_VIEW, pos, replace, ifNotExists);
        this.name = name;
        this.query = markInnerSelectsAsNonExpandable(query);
    }

    public String name() {
        return name.toString();
    }

    public SqlNode getQuery() {
        return query;
    }

    /**
     * @return expanded columns projection.
     * It is extremely helpful to determine columns name
     * in newly-created view as virtual table.
     */
    public List<String> projection() {
        List<String> projection = new ArrayList<>();
        for (SqlSelect select : innerSelects) {
            for (int i = 0; i < select.getSelectList().size(); ++i) {
                String field = select.getSelectList().get(i).toString();
                // split "map.__key" to ["map", "__key"].
                // We want to create a separate scope for created view.
                String[] splittedField = field.split("\\.");
                if (splittedField.length > 0) {
                    projection.add(splittedField[splittedField.length - 1]);
                }
            }
        }
        return projection;
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

    private SqlNode markInnerSelectsAsNonExpandable(SqlNode query) {
        if (query instanceof SqlSelect) {
            SqlSelect select = new SqlNonExpandableSelect((SqlSelect) query);
            innerSelects.add(select);
            return select;
        }

        if (query instanceof SqlOrderBy) {
            SqlOrderBy orderBy = (SqlOrderBy) query;
            if (orderBy.query instanceof SqlSelect) {
                SqlSelect select = new SqlNonExpandableSelect((SqlSelect) orderBy.query);
                innerSelects.add(select);
                return new SqlOrderBy(
                        orderBy.getParserPosition(),
                        select,
                        orderBy.orderList,
                        orderBy.offset,
                        orderBy.fetch
                );
            }
        }

        if (query instanceof SqlBasicCall) {
            SqlBasicCall call = (SqlBasicCall) query;
            for (int i = 0; i < call.getOperandList().size(); ++i) {
                SqlNode node = call.getOperandList().get(i);
                call.setOperand(i, markInnerSelectsAsNonExpandable(node));
            }
            return call;
        }
        return query;
    }
}
