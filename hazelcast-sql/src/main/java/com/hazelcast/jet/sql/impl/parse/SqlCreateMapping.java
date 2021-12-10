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

import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.sql.impl.schema.Mapping;
import com.hazelcast.sql.impl.schema.MappingField;
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
import static com.hazelcast.jet.sql.impl.validate.ValidationUtil.isCatalogObjectNameValid;
import static java.util.Objects.requireNonNull;

public class SqlCreateMapping extends SqlCreate {

    private static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("CREATE EXTERNAL MAPPING", SqlKind.CREATE_TABLE);

    private final SqlIdentifier name;
    private final SqlIdentifier externalName;
    private final SqlNodeList columns;
    private final SqlIdentifier type;
    private final SqlNodeList options;

    public SqlCreateMapping(
            SqlIdentifier name,
            SqlIdentifier externalName,
            SqlNodeList columns,
            SqlIdentifier type,
            SqlNodeList options,
            boolean replace,
            boolean ifNotExists,
            SqlParserPos pos
    ) {
        super(OPERATOR, pos, replace, ifNotExists);

        this.name = requireNonNull(name, "Name should not be null");
        this.externalName = externalName;
        this.columns = requireNonNull(columns, "Columns should not be null");
        this.type = requireNonNull(type, "Type should not be null");
        this.options = requireNonNull(options, "Options should not be null");

        Preconditions.checkTrue(
                externalName == null || externalName.isSimple(),
                externalName == null ? null : externalName.toString()
        );
    }

    public String nameWithoutSchema() {
        return name.names.get(name.names.size() - 1);
    }

    public String externalName() {
        return externalName == null ? nameWithoutSchema() : externalName.getSimple();
    }

    public Stream<SqlMappingColumn> columns() {
        return columns.getList().stream().map(node -> (SqlMappingColumn) node);
    }

    public String type() {
        return type.toString();
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
        return ImmutableNullableList.of(name, columns, type, options);
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

        writer.newlineAndIndent();
        writer.keyword("TYPE");
        type.unparse(writer, leftPrec, rightPrec);

        if (options.size() > 0) {
            writer.newlineAndIndent();
            writer.keyword("OPTIONS");
            SqlWriter.Frame withFrame = writer.startList("(", ")");
            for (SqlNode property : options) {
                printIndent(writer);
                property.unparse(writer, leftPrec, rightPrec);
            }
            writer.newlineAndIndent();
            writer.endList(withFrame);
        }
    }

    public static String unparse(Mapping mapping) {
        SqlPrettyWriter writer = new SqlPrettyWriter(SqlPrettyWriter.config());

        writer.keyword("CREATE MAPPING");
        writer.identifier(mapping.name(), true);

        // external name defaults to mapping name - omit it if it's equal
        if (mapping.externalName() != null && !mapping.externalName().equals(mapping.name())) {
            writer.keyword("EXTERNAL NAME");
            writer.identifier(mapping.externalName(), true);
        }

        List<MappingField> fields = mapping.fields();
        if (fields.size() > 0) {
            SqlWriter.Frame frame = writer.startList("(", ")");
            for (MappingField field : fields) {
                printIndent(writer);
                writer.identifier(field.name(), true);
                writer.print(field.type().getTypeFamily().toString());
                if (field.externalName() != null) {
                    writer.print(" ");
                    writer.keyword("EXTERNAL NAME");
                    writer.identifier(field.externalName(), true);
                }
            }
            writer.newlineAndIndent();
            writer.endList(frame);
        }

        writer.newlineAndIndent();
        writer.keyword("TYPE");
        writer.print(mapping.type());

        Map<String, String> options = mapping.options();
        if (options.size() > 0) {
            writer.newlineAndIndent();
            writer.keyword("OPTIONS");
            SqlWriter.Frame withFrame = writer.startList("(", ")");
            for (Map.Entry<String, String> option : options.entrySet()) {
                printIndent(writer);
                writer.literal(writer.getDialect().quoteStringLiteral(option.getKey()));
                writer.print("= ");
                writer.literal(writer.getDialect().quoteStringLiteral(option.getValue()));
            }
            writer.newlineAndIndent();
            writer.endList(withFrame);
        }

        return writer.toString();
    }

    private static void printIndent(SqlWriter writer) {
        writer.sep(",", false);
        writer.newlineAndIndent();
        writer.print("  ");
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        if (getReplace() && ifNotExists) {
            throw validator.newValidationError(this, RESOURCE.orReplaceWithIfNotExistsNotSupported());
        }

        if (!isCatalogObjectNameValid(name)) {
            throw validator.newValidationError(name, RESOURCE.mappingIncorrectSchema());
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
