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

package com.hazelcast.jet.sql.impl.parse;

import com.google.common.collect.ImmutableList;
import com.hazelcast.jet.sql.impl.validate.ValidationUtil;
import com.hazelcast.jet.sql.impl.validate.operators.special.HazelcastCreateDataConnectionOperator;
import com.hazelcast.sql.impl.schema.dataconnection.DataConnectionCatalogEntry;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import javax.annotation.Nonnull;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.sql.impl.parse.ParserResource.RESOURCE;
import static com.hazelcast.jet.sql.impl.parse.UnparseUtil.identifier;
import static com.hazelcast.jet.sql.impl.parse.UnparseUtil.nodeList;
import static com.hazelcast.jet.sql.impl.parse.UnparseUtil.unparseOptions;
import static com.hazelcast.sql.impl.QueryUtils.CATALOG;
import static com.hazelcast.sql.impl.QueryUtils.SCHEMA_NAME_PUBLIC;

/**
 * AST node representing a CREATE DATA CONNECTION statement.
 *
 * @since 5.3
 */
public class SqlCreateDataConnection extends SqlCreate {
    public static final SqlOperator CREATE_DATA_CONNECTION = new HazelcastCreateDataConnectionOperator();

    private final SqlIdentifier name;
    private final SqlIdentifier type;
    private final boolean shared;
    private final SqlNodeList options;

    public SqlCreateDataConnection(
            SqlParserPos pos,
            boolean replace,
            boolean ifNotExists,
            @Nonnull SqlIdentifier name,
            @Nonnull SqlIdentifier type,
            boolean shared,
            @Nonnull SqlNodeList options) {
        super(CREATE_DATA_CONNECTION, pos, replace, ifNotExists);
        this.name = name;
        this.type = type;
        this.shared = shared;
        this.options = options;
    }

    public String nameWithoutSchema() {
        return name.names.get(name.names.size() - 1);
    }

    public String type() {
        return type.toString();
    }

    public boolean shared() {
        return shared;
    }

    public Map<String, String> options() {
        return options.getList().stream()
                .map(node -> (SqlOption) node)
                .collect(
                        LinkedHashMap::new,
                        (map, option) -> map.putIfAbsent(option.keyString(), option.valueString()),
                        Map::putAll
                );
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(name, type, options);
    }

    @Override
    public SqlOperator getOperator() {
        return CREATE_DATA_CONNECTION;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (getReplace()) {
            writer.keyword("CREATE OR REPLACE");
        } else {
            writer.keyword("CREATE");
        }
        writer.keyword("DATA CONNECTION");

        if (ifNotExists) {
            writer.keyword("IF NOT EXISTS");
        }

        name.unparse(writer, leftPrec, rightPrec);

        writer.newlineAndIndent();
        writer.keyword("TYPE");
        type.unparse(writer, leftPrec, rightPrec);

        writer.newlineAndIndent();
        if (!shared) {
            writer.keyword("NOT");
        }
        writer.keyword("SHARED");

        unparseOptions(writer, options);
    }

    @Nonnull
    public static String unparse(@Nonnull DataConnectionCatalogEntry dataConnection) {
        SqlPrettyWriter writer = new SqlPrettyWriter(SqlPrettyWriter.config());

        SqlCreateDataConnection d = new SqlCreateDataConnection(
                SqlParserPos.ZERO, true, false,
                identifier(CATALOG, SCHEMA_NAME_PUBLIC, dataConnection.name()),
                identifier(dataConnection.type()),
                dataConnection.isShared(),
                nodeList(dataConnection.options().entrySet(), o -> new SqlOption(
                        SqlLiteral.createCharString(o.getKey(), SqlParserPos.ZERO),
                        SqlLiteral.createCharString(o.getValue(), SqlParserPos.ZERO),
                        SqlParserPos.ZERO
                )));

        d.unparse(writer, 0, 0);
        return writer.toString();
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        if (getReplace() && ifNotExists) {
            throw validator.newValidationError(this, RESOURCE.orReplaceWithIfNotExistsNotSupported());
        }

        if (!ValidationUtil.isCatalogObjectNameValid(name)) {
            throw validator.newValidationError(this, RESOURCE.dataConnectionIncorrectSchemaCreate());
        }
    }
}
