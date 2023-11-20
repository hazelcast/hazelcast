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

import com.hazelcast.jet.impl.util.Util;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;

import static java.util.Arrays.asList;

public final class UnparseUtil {

    private UnparseUtil() { }

    @Nullable
    public static SqlIdentifier identifier(String ... names) {
        if (names == null || names.length == 1 && names[0] == null) {
            return null;
        }
        return new SqlIdentifier(asList(names), SqlParserPos.ZERO);
    }

    public static void printIndent(SqlWriter writer) {
        writer.sep(",", false);
        writer.newlineAndIndent();
        writer.print("  ");
    }

    public static void unparseOptions(SqlWriter writer, SqlNodeList options) {
        unparseOptions(writer, "OPTIONS", options);
    }

    public static void unparseOptions(SqlWriter writer, String prefix, SqlNodeList options) {
        if (options != null && options.size() > 0) {
            writer.newlineAndIndent();
            writer.keyword(prefix);
            SqlWriter.Frame withFrame = writer.startList("(", ")");
            for (SqlNode property : options) {
                printIndent(writer);
                property.unparse(writer, 0, 0);
            }
            writer.newlineAndIndent();
            writer.endList(withFrame);
        }
    }

    public static SqlNodeList reconstructOptions(Map<String, String> options) {
        return nodeList(options.entrySet(), o -> new SqlOption(
                SqlLiteral.createCharString(o.getKey(), SqlParserPos.ZERO),
                SqlLiteral.createCharString(o.getValue(), SqlParserPos.ZERO),
                SqlParserPos.ZERO));
    }

    public static <T> SqlNodeList nodeList(Collection<T> collection, Function<T, SqlNode> mapFn) {
        return new SqlNodeList(Util.toList(collection, mapFn), SqlParserPos.ZERO);
    }
}
