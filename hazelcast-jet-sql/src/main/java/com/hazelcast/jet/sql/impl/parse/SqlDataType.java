/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.sql.impl.type.QueryDataTypeUtils;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import static java.util.Objects.requireNonNull;

public class SqlDataType extends SqlIdentifier {

    private final QueryDataType type;

    public SqlDataType(QueryDataType type, SqlParserPos pos) {
        super(type.toString(), pos);

        this.type = requireNonNull(type);
    }

    public QueryDataType type() {
        return type;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(QueryDataTypeUtils.sqlTypeName(type));
    }
}
