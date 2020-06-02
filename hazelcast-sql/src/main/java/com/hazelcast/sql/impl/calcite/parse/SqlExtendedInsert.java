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

package com.hazelcast.sql.impl.calcite.parse;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlExtendedInsert extends SqlInsert {

    private final SqlNodeList extendedKeywords;

    public SqlExtendedInsert(SqlNode table,
                             SqlNode source,
                             SqlNodeList keywords,
                             SqlNodeList extendedKeywords,
                             SqlNodeList columns,
                             SqlParserPos pos) {
        super(pos, keywords, table, source, columns);

        this.extendedKeywords = extendedKeywords;
    }

    public ImmutableList<String> tableNames() {
        return ((SqlIdentifier) getTargetTable()).names;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        String insertKeyword = "INSERT INTO";
        if (isOverwrite()) {
            insertKeyword = "INSERT OVERWRITE";
        }
        writer.sep(insertKeyword);
        int opLeft = getOperator().getLeftPrec();
        int opRight = getOperator().getRightPrec();
        getTargetTable().unparse(writer, opLeft, opRight);
        if (getTargetColumnList() != null) {
            getTargetColumnList().unparse(writer, opLeft, opRight);
        }
        writer.newlineAndIndent();
        getSource().unparse(writer, 0, 0);
    }

    public boolean isOverwrite() {
        for (SqlNode keyword : extendedKeywords) {
            if (((SqlLiteral) keyword).symbolValue(SqlExtendedInsertKeyword.class) == SqlExtendedInsertKeyword.OVERWRITE) {
                return true;
            }
        }
        return false;
    }
}
