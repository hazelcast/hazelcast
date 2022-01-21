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

import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.sql.impl.QueryException;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.jet.sql.impl.parse.ParserResource.RESOURCE;
import static java.util.Objects.requireNonNull;

/**
 * CREATE INDEX [ IF NOT EXISTS ] name ON map_name ( attribute_name, ... )
 * [ TYPE index_type ]
 * [ OPTIONS ( 'option_name' = 'option_value' [, ...] ) ]
 */
public class SqlCreateIndex extends SqlCreate {
    public static final String UNIQUE_KEY = "unique_key";
    public static final String UNIQUE_KEY_TRANSFORMATION = "unique_key_transformation";

    private static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("CREATE INDEX", SqlKind.CREATE_INDEX);

    private final SqlIdentifier name;
    private final SqlIdentifier mapName;
    private final SqlNodeList columns;
    private final SqlIdentifier type;
    private final SqlNodeList options;

    public SqlCreateIndex(
            SqlIdentifier name,
            SqlIdentifier mapName,
            SqlNodeList columns,
            SqlIdentifier type,
            SqlNodeList options,
            boolean replace,
            boolean ifNotExists,
            SqlParserPos pos
    ) {
        super(OPERATOR, pos, replace, ifNotExists);

        this.name = requireNonNull(name, "Name should not be null");
        this.mapName = requireNonNull(mapName, "Map name should not be null");
        this.columns = requireNonNull(columns, "Columns should not be null");
        this.type = type;
        this.options = requireNonNull(options, "Options should not be null");
    }

    public List<String> columns() {
        return Util.toList(columns, n -> ((SqlIdentifier) n).getSimple());
    }

    public IndexType type() {
        return getIndexType();
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

    public String mapName() {
        return mapName.toString();
    }

    public String indexName() {
        return name.toString();
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, columns, type, options);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE INDEX");

        if (ifNotExists) {
            writer.keyword("IF NOT EXISTS");
        }

        name.unparse(writer, leftPrec, rightPrec);

        writer.keyword("ON");

        mapName.unparse(writer, leftPrec, rightPrec);

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

    private static void printIndent(SqlWriter writer) {
        writer.sep(",", false);
        writer.newlineAndIndent();
        writer.print("  ");
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        if (getReplace()) {
            throw validator.newValidationError(this, RESOURCE.notSupported("OR REPLACE", "CREATE INDEX"));
        }

        Set<String> columnNames = new HashSet<>();
        for (SqlNode column : columns.getList()) {
            String name = ((SqlIdentifier) requireNonNull(column)).getSimple();
            if (!columnNames.add(name)) {
                throw validator.newValidationError(column, RESOURCE.duplicateIndexAttribute(name));
            }
        }

        IndexType indexType = getIndexType();
        if (!indexType.equals(IndexType.BITMAP) && !options.getList().isEmpty()) {
            throw validator.newValidationError(options, RESOURCE.unsupportedIndexType(indexType.name(),
                    options().keySet().iterator().next()));
        }

        Set<String> optionNames = new HashSet<>();

        for (SqlNode option : options.getList()) {
            String name = ((SqlOption) option).keyString();
            if (!optionNames.add(name)) {
                throw validator.newValidationError(option, RESOURCE.duplicateOption(name));
            }
        }
    }

    /**
     * @return the index type, if specified, the default index type if not.
     */
    @Nonnull
    private IndexType getIndexType() {
        if (type == null) {
            return IndexConfig.DEFAULT_TYPE;
        }
        String indexType = type.toString().toLowerCase();
        IndexType type;
        switch (indexType) {
            case "sorted":
                type = IndexType.SORTED;
                break;
            case "hash":
                type = IndexType.HASH;
                break;
            case "bitmap":
                type = IndexType.BITMAP;
                break;
            default:
                throw QueryException.error(
                        "Can't create index: wrong index type. Only HASH, SORTED and BITMAP types are supported."
                );
        }
        return type;
    }
}
