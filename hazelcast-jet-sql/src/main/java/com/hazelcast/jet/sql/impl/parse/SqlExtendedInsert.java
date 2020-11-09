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

package com.hazelcast.jet.sql.impl.parse;

import com.google.common.collect.ImmutableList;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorTable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.hazelcast.jet.sql.impl.connector.SqlConnectorUtil.getJetSqlConnector;
import static com.hazelcast.jet.sql.impl.parse.ParserResource.RESOURCE;

public class SqlExtendedInsert extends SqlInsert {

    private final SqlNodeList extendedKeywords;
    private SqlNodeList overrideColumnList;

    public SqlExtendedInsert(
            SqlNode table,
            SqlNode source,
            SqlNodeList keywords,
            SqlNodeList extendedKeywords,
            SqlNodeList columns,
            SqlParserPos pos
    ) {
        super(pos, keywords, table, source, columns);

        this.extendedKeywords = extendedKeywords;
    }

    public ImmutableList<String> tableNames() {
        return ((SqlIdentifier) getTargetTable()).names;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        if (isSink()) {
            writer.keyword("SINK INTO");
        } else {
            writer.keyword("INSERT INTO");
        }
        int opLeft = getOperator().getLeftPrec();
        int opRight = getOperator().getRightPrec();
        getTargetTable().unparse(writer, opLeft, opRight);
        if (getTargetColumnList() != null) {
            getTargetColumnList().unparse(writer, opLeft, opRight);
        }
        writer.newlineAndIndent();
        getSource().unparse(writer, 0, 0);
    }

    @Override
    public SqlNodeList getTargetColumnList() {
        return overrideColumnList != null ? overrideColumnList
                : super.getTargetColumnList();
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        SqlValidatorTable table0 = validator.getCatalogReader().getTable(tableNames());
        if (table0 == null) {
            super.validate(validator, scope);
            assert false; // should have failed with "Object not found"
        }

        HazelcastTable table = table0.unwrap(HazelcastTable.class);
        if (getTargetColumnList() == null) {
            RelDataType rowType = table.getRowType(validator.getTypeFactory());
            List<SqlNode> columnListWithoutHidden = new ArrayList<>();
            for (RelDataTypeField f : rowType.getFieldList()) {
                if (!table.isHidden(f.getName())) {
                    columnListWithoutHidden.add(new SqlIdentifier(f.getName(), SqlParserPos.ZERO));
                }
            }
            overrideColumnList = new SqlNodeList(columnListWithoutHidden, SqlParserPos.ZERO);
        }

        super.validate(validator, scope);

        Map<String, TableField> fieldsMap = table.getTarget().getFields().stream()
                                                 .map(f -> (MapTableField) f)
                                                 .collect(Collectors.toMap(MapTableField::getName, f -> f));

        for (SqlNode fieldNode : getTargetColumnList()) {
            TableField field = fieldsMap.get(((SqlIdentifier) fieldNode).getSimple());
            if (field instanceof MapTableField) {
                QueryPath path = ((MapTableField) field).getPath();
                if (path.getPath() == null
                        && field.getType().getTypeFamily() == QueryDataTypeFamily.OBJECT) {
                    throw validator.newValidationError(fieldNode, RESOURCE.insertToTopLevelObject());
                }
            }
        }

        SqlConnector connector = getJetSqlConnector(table.getTarget());
        if (isSink()) {
            if (!connector.supportsSink()) {
                throw validator.newValidationError(this, RESOURCE.sinkIntoNotSupported(connector.typeName()));
            }
        } else {
            if (!connector.supportsInsert()) {
                throw validator.newValidationError(this, RESOURCE.insertIntoNotSupported(connector.typeName()));
            }
        }
    }

    private boolean isSink() {
        for (SqlNode keyword : extendedKeywords) {
            if (((SqlLiteral) keyword).symbolValue(Keyword.class) == Keyword.SINK) {
                return true;
            }
        }
        return false;
    }

    public enum Keyword {
        SINK;

        public SqlLiteral symbol(SqlParserPos pos) {
            return SqlLiteral.createSymbol(this, pos);
        }
    }
}
