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

import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlDataType extends SqlIdentifier {

    private final QueryDataType type;

    public SqlDataType(QueryDataType type, SqlParserPos pos) {
        super(type.toString(), pos);
        this.type = type;
    }

    public QueryDataType type() {
        return type;
    }

    @Override
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        switch (type.getTypeFamily()) {
            case BOOLEAN:
                writer.keyword("BOOLEAN");
                break;
            case TINYINT:
                writer.keyword("TINYINT");
                break;
            case SMALLINT:
                writer.keyword("SMALLINT");
                break;
            case INT:
                writer.keyword("INT");
                break;
            case BIGINT:
                writer.keyword("BIGINT");
                break;
            case REAL:
                writer.keyword("REAL");
                break;
            case DOUBLE:
                writer.keyword("DOUBLE");
                break;
            case DECIMAL:
                writer.keyword("DECIMAL");
                break;
            case VARCHAR:
                writer.keyword("VARCHAR");
                break;
            case TIME:
                writer.keyword("TIME");
                break;
            case DATE:
                writer.keyword("DATE");
                break;
            case TIMESTAMP:
                writer.keyword("TIMESTAMP");
                break;
            case TIMESTAMP_WITH_TIME_ZONE:
                writer.keyword("TIMESTAMP WITH TIME ZONE");
                break;
            case OBJECT:
                writer.keyword("OBJECT");
                break;
            default:
                throw new RuntimeException("Unsupported type family: " + type.getTypeFamily());
        }
    }
}
