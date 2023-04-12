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

import com.hazelcast.sql.impl.schema.Mapping;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static com.hazelcast.jet.sql.impl.parse.ParserResource.RESOURCE;
import static com.hazelcast.jet.sql.impl.parse.UnparseUtil.identifier;
import static com.hazelcast.jet.sql.impl.parse.UnparseUtil.nodeList;
import static com.hazelcast.jet.sql.impl.parse.UnparseUtil.printIndent;
import static com.hazelcast.jet.sql.impl.parse.UnparseUtil.reconstructOptions;
import static com.hazelcast.jet.sql.impl.parse.UnparseUtil.unparseOptions;
import static com.hazelcast.jet.sql.impl.validate.ValidationUtil.isCatalogObjectNameValid;
import static com.hazelcast.sql.impl.QueryUtils.CATALOG;
import static com.hazelcast.sql.impl.QueryUtils.SCHEMA_NAME_PUBLIC;
import static java.util.Objects.requireNonNull;

public class SqlCreateMapping extends SqlCreate {

    private static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("CREATE EXTERNAL MAPPING", SqlKind.CREATE_TABLE);

    private final SqlIdentifier name;
    private final SqlIdentifier externalName;
    private final SqlNodeList columns;
    private final SqlIdentifier dataConnection;
    private final SqlIdentifier connectorType;
    private final SqlIdentifier objectType;
    private final SqlNodeList options;

    public SqlCreateMapping(
            SqlIdentifier name,
            SqlIdentifier externalName,
            SqlNodeList columns,
            SqlIdentifier dataConnection,
            SqlIdentifier connectorType,
            SqlIdentifier objectType,
            SqlNodeList options,
            boolean replace,
            boolean ifNotExists,
            SqlParserPos pos
    ) {
        super(OPERATOR, pos, replace, ifNotExists);

        this.name = requireNonNull(name, "Name should not be null");
        this.externalName = externalName;
        this.columns = requireNonNull(columns, "Columns should not be null");
        this.dataConnection = dataConnection;
        this.connectorType = connectorType;
        this.objectType = objectType;
        this.options = requireNonNull(options, "Options should not be null");

        assert dataConnection == null || connectorType == null; // the syntax doesn't allow this
    }

    public String nameWithoutSchema() {
        return name.names.get(name.names.size() - 1);
    }

    public String[] externalName() {
        return externalName == null ? new String[]{nameWithoutSchema()} : externalName.names.toArray(new String[0]);
    }

    public Stream<SqlMappingColumn> columns() {
        return columns.getList().stream().map(node -> (SqlMappingColumn) node);
    }

    public String dataConnectionNameWithoutSchema() {
        if (dataConnection == null) {
            return null;
        }
        return dataConnection.names.get(dataConnection.names.size() - 1);
    }

    public String connectorType() {
        return connectorType != null ? connectorType.toString() : null;
    }

    public String objectType() {
        return objectType != null ? objectType.toString() : null;
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

    public boolean ifNotExists() {
        return ifNotExists;
    }

    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, columns, dataConnection, connectorType, objectType, options);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");

        if (getReplace()) {
            writer.keyword("OR REPLACE");
        }

        writer.keyword("EXTERNAL MAPPING");

        if (ifNotExists) {
            writer.keyword("IF NOT EXISTS");
        }

        name.unparse(writer, leftPrec, rightPrec);
        if (externalName != null) {
            writer.keyword("EXTERNAL NAME");
            externalName.unparse(writer, leftPrec, rightPrec);
        }

        if (columns.size() > 0) {
            SqlWriter.Frame frame = writer.startList("(", ")");
            for (SqlNode column : columns) {
                printIndent(writer);
                column.unparse(writer, 0, 0);
            }
            writer.newlineAndIndent();
            writer.endList(frame);
        }

        if (dataConnection != null) {
            writer.newlineAndIndent();
            writer.keyword("DATA CONNECTION");
            dataConnection.unparse(writer, leftPrec, rightPrec);
        } else {
            assert connectorType != null;
            writer.newlineAndIndent();
            writer.keyword("TYPE");
            connectorType.unparse(writer, leftPrec, rightPrec);
        }

        if (objectType != null) {
            writer.newlineAndIndent();
            writer.keyword("OBJECT TYPE");
            objectType.unparse(writer, leftPrec, rightPrec);
        }

        unparseOptions(writer, options);
    }

    public static String unparse(Mapping mapping) {
        SqlPrettyWriter writer = new SqlPrettyWriter(SqlPrettyWriter.config());

        SqlCreateMapping m = new SqlCreateMapping(
                identifier(CATALOG, SCHEMA_NAME_PUBLIC, mapping.name()),
                identifier(mapping.externalName()),
                nodeList(mapping.fields(), f -> new SqlMappingColumn(
                        identifier(f.name()),
                        new SqlDataType(f.type(), SqlParserPos.ZERO),
                        identifier(f.externalName()),
                        SqlParserPos.ZERO)),
                identifier(mapping.dataConnection()),
                identifier(mapping.connectorType()),
                identifier(mapping.objectType()),
                reconstructOptions(mapping.options()),
                true, false, SqlParserPos.ZERO
        );

        m.unparse(writer, 0, 0);
        return writer.toString();
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        if (getReplace() && ifNotExists) {
            throw validator.newValidationError(this, RESOURCE.orReplaceWithIfNotExistsNotSupported());
        }

        if (!isCatalogObjectNameValid(name)) {
            throw validator.newValidationError(name, RESOURCE.mappingIncorrectSchema());
        }

        if (dataConnection != null && !isCatalogObjectNameValid(dataConnection)) {
            throw validator.newValidationError(name, RESOURCE.dataConnectionIncorrectSchemaUse());
        }

        Set<String> columnNames = new HashSet<>();
        for (SqlNode column : columns.getList()) {
            String name = ((SqlMappingColumn) column).name();
            if (!columnNames.add(name)) {
                throw validator.newValidationError(column, RESOURCE.duplicateColumn(name));
            }
        }

        Set<String> optionNames = new HashSet<>();
        for (SqlNode option : options.getList()) {
            String name = ((SqlOption) option).keyString();
            if (!optionNames.add(name)) {
                throw validator.newValidationError(option, RESOURCE.duplicateOption(name));
            }
        }
    }
}
