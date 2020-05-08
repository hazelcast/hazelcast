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
        // TODO: add `name` to QueryDataType ?
        if (type == QueryDataType.BOOLEAN) {
            writer.keyword("BOOLEAN");
        } else if (type == QueryDataType.TINYINT) {
            writer.keyword("TINYINT");
        } else if (type == QueryDataType.SMALLINT) {
            writer.keyword("SMALLINT");
        } else if (type == QueryDataType.INT) {
            writer.keyword("INT");
        } else if (type == QueryDataType.BIGINT) {
            writer.keyword("BIGINT");
        } else if (type == QueryDataType.REAL) {
            writer.keyword("REAL");
        } else if (type == QueryDataType.DOUBLE) {
            writer.keyword("DOUBLE");
        } else if (type == QueryDataType.DECIMAL) {
            writer.keyword("DECIMAL");
        } else if (type == QueryDataType.DECIMAL_BIG_INTEGER) {
            writer.keyword("DECIMAL");
        } else if (type == QueryDataType.VARCHAR_CHARACTER) {
            writer.keyword("CHAR");
        } else if (type == QueryDataType.VARCHAR) {
            writer.keyword("VARCHAR");
        } else if (type == QueryDataType.TIME) {
            writer.keyword("TIME");
        } else if (type == QueryDataType.DATE) {
            writer.keyword("DATE");
        } else if (type == QueryDataType.TIMESTAMP) {
            writer.keyword("TIMESTAMP");
        } else if (type == QueryDataType.TIMESTAMP_WITH_TZ_DATE) {
            writer.keyword("TIMESTAMP WITH LOCAL TIME ZONE (\"DATE\")");
        } else if (type == QueryDataType.TIMESTAMP_WITH_TZ_CALENDAR) {
            writer.keyword("TIMESTAMP WITH TIME ZONE (\"CALENDAR\")");
        } else if (type == QueryDataType.TIMESTAMP_WITH_TZ_INSTANT) {
            writer.keyword("TIMESTAMP WITH LOCAL TIME ZONE");
        } else if (type == QueryDataType.TIMESTAMP_WITH_TZ_ZONED_DATE_TIME) {
            writer.keyword("TIMESTAMP WITH TIME ZONE (\"ZONED_DATE_TIME\")");
        } else if (type == QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME) {
            writer.keyword("TIMESTAMP WITH TIME ZONE");
        }
    }
}
