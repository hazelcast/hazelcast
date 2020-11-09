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

import com.google.common.collect.ImmutableMap;
import com.hazelcast.jet.sql.impl.calcite.parser.JetSqlParser;
import com.hazelcast.sql.impl.type.QueryDataType;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParser.Config;
import org.junit.Test;
import org.junit.runner.RunWith;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(JUnitParamsRunner.class)
public class JetSqlParserTest {

    private static final Config CONFIG = SqlParser.configBuilder()
                                                  .setCaseSensitive(true)
                                                  .setUnquotedCasing(Casing.UNCHANGED)
                                                  .setQuotedCasing(Casing.UNCHANGED)
                                                  .setQuoting(Quoting.DOUBLE_QUOTE)
                                                  .setParserFactory(JetSqlParser.FACTORY)
                                                  .build();

    @Test
    @Parameters({
            "true, false",
            "false, true"
    })
    public void test_createMapping(boolean replace, boolean ifNotExists) throws SqlParseException {
        // given
        String sql = "CREATE "
                + (replace ? "OR REPLACE " : "")
                + "MAPPING "
                + (ifNotExists ? "IF NOT EXISTS " : "")
                + "mapping_name ("
                + "column_name INT EXTERNAL NAME \"external.name\""
                + ")"
                + "TYPE mapping_type "
                + "OPTIONS("
                + "\"option.key\" 'option.value'"
                + ")";

        // when
        SqlCreateMapping node = (SqlCreateMapping) parse(sql);

        // then
        assertThat(node.nameWithoutSchema()).isEqualTo("mapping_name");
        assertThat(node.type()).isEqualTo("mapping_type");
        assertThat(node.columns().findFirst())
                .isNotEmpty().get()
                .extracting(column -> new Object[]{column.name(), column.type(), column.externalName()})
                .isEqualTo(new Object[]{"column_name", QueryDataType.INT, "external.name"});
        assertThat(node.options()).isEqualTo(ImmutableMap.of("option.key", "option.value"));
        assertThat(node.getReplace()).isEqualTo(replace);
        assertThat(node.ifNotExists()).isEqualTo(ifNotExists);
    }

    @Test
    public void test_createExternalMapping() throws SqlParseException {
        // given
        String sql = "CREATE EXTERNAL MAPPING "
                + "mapping_name "
                + "TYPE mapping_type";

        // when
        SqlNode node = parse(sql);

        // then
        assertThat(node).isInstanceOf(SqlCreateMapping.class);
    }

    @Test
    public void test_createExternalMappingWithSchema() throws SqlParseException {
        // given
        String sql = "CREATE MAPPING "
                + "public.mapping_name "
                + "TYPE mapping_type";

        // when
        SqlCreateMapping node = (SqlCreateMapping) parse(sql);

        // then
        assertThat(node.nameWithoutSchema()).isEqualTo("mapping_name");
    }

    @Test
    public void test_createMappingRequiresColumns() {
        // given
        String sql = "CREATE MAPPING "
                + "mapping_name ("
                + ")"
                + "TYPE mapping_type";

        // when
        assertThatThrownBy(() -> parse(sql))
                .hasMessageContaining("Encountered \")\" at line 1, column 30");
    }

    @Test
    public void test_createMappingRequiresUniqueColumnName() {
        // given
        String sql = "CREATE MAPPING "
                + "mapping_name ("
                + "column_name INT"
                + ", column_name VARCHAR"
                + ")"
                + "TYPE mapping_type";

        // when
        assertThatThrownBy(() -> parse(sql))
                .hasMessageContaining("Column 'column_name' specified more than once");
    }

    @Test
    public void test_createMappingRequiresColumnType() {
        // given
        String sql = "CREATE MAPPING "
                + "mapping_name ("
                + "column_name"
                + ")"
                + "TYPE mapping_type";

        // when
        assertThatThrownBy(() -> parse(sql))
                .hasMessageContaining("Encountered \")\" at line 1, column 41");
    }

    @Test
    public void test_createMappingRequiresMappingType() {
        // given
        String sql = "CREATE MAPPING "
                + "mapping_name ("
                + "column_name INT"
                + ")";

        // when
        assertThatThrownBy(() -> parse(sql))
                .hasMessageContaining("Encountered \"<EOF>\" at line 1, column 45");
    }

    @Test
    public void test_createMappingRequiresMappingTypeSimpleIdentifier() {
        // given
        String sql = "CREATE MAPPING "
                + "mapping_name "
                + "TYPE mapping.type";

        // when
        assertThatThrownBy(() -> parse(sql))
                .hasMessageContaining("Encountered \".\" at line 1, column 41");
    }

    @Test
    public void test_createMappingRequiresUniqueOptionKey() {
        // given
        String sql = "CREATE MAPPING "
                + "mapping_name "
                + "TYPE mapping_type "
                + "OPTIONS("
                + "\"option.key\" 'value1'"
                + ", \"option.key\" 'value2'"
                + ")";

        // when
        assertThatThrownBy(() -> parse(sql))
                .hasMessageContaining("Option 'option.key' specified more than once");
    }

    @Test
    public void test_createMappingRequiresOptionSimpleIdentifier() {
        // given
        String sql = "CREATE MAPPING "
                + "mapping_name "
                + "TYPE mapping_type "
                + "OPTIONS ("
                + "option.key 'value1'"
                + ")";

        // when
        assertThatThrownBy(() -> parse(sql))
                .hasMessageContaining("Encountered \".\" at line 1, column 62");
    }

    @Test
    @Parameters({
            "false",
            "true"
    })
    public void test_dropMapping(boolean ifExists) throws SqlParseException {
        // given
        String sql = "DROP MAPPING " + (ifExists ? "IF EXISTS " : "") + "mapping_name";

        // when
        SqlDropMapping node = (SqlDropMapping) parse(sql);

        // then
        assertThat(node.nameWithoutSchema()).isEqualTo("mapping_name");
        assertThat(node.ifExists()).isEqualTo(ifExists);
    }

    @Test
    public void test_dropExternalMapping() throws SqlParseException {
        // given
        String sql = "DROP EXTERNAL MAPPING mapping_name";

        // when
        SqlNode node = parse(sql);

        // then
        assertThat(node).isInstanceOf(SqlDropMapping.class);
    }

    @Test
    public void test_dropExternalMappingWithSchema() throws SqlParseException {
        // given
        String sql = "DROP MAPPING some_schema.mapping_name";

        // when
        SqlDropMapping node = (SqlDropMapping) parse(sql);

        // then
        assertThat(node.nameWithoutSchema()).isEqualTo("mapping_name");
    }

    @Test
    public void test_sinkIntoWithValues() throws SqlParseException {
        // given
        String sql = "SINK INTO partitioned.t (id, name) VALUES (1, '1')";

        // when
        SqlExtendedInsert node = (SqlExtendedInsert) parse(sql);

        // then
        assertThat(node.tableNames()).isEqualTo(asList("partitioned", "t"));
        assertThat(node.getSource().getKind()).isEqualTo(SqlKind.VALUES);
        assertThat(node.getTargetColumnList()).hasSize(2);
    }

    @Test
    public void test_sinkIntoWithSelect() throws SqlParseException {
        // given
        String sql = "SINK INTO t SELECT * FROM s";

        // when
        SqlExtendedInsert node = (SqlExtendedInsert) parse(sql);

        // then
        assertThat(node.tableNames()).isEqualTo(singletonList("t"));
        assertThat(node.getSource().getKind()).isEqualTo(SqlKind.SELECT);
        assertThat(node.getTargetColumnList()).isNullOrEmpty();
    }

    @Test
    public void test_insertIntoWithValues() throws SqlParseException {
        // given
        String sql = "INSERT INTO partitioned.t (id, name) VALUES (1, '1')";

        // when
        SqlExtendedInsert node = (SqlExtendedInsert) parse(sql);

        // then
        assertThat(node.tableNames()).isEqualTo(asList("partitioned", "t"));
        assertThat(node.getSource().getKind()).isEqualTo(SqlKind.VALUES);
        assertThat(node.getTargetColumnList()).hasSize(2);
    }

    @Test
    public void test_insertIntoWithSelect() throws SqlParseException {
        // given
        String sql = "INSERT INTO t SELECT * FROM s";

        // when
        SqlExtendedInsert node = (SqlExtendedInsert) parse(sql);

        // then
        assertThat(node.tableNames()).isEqualTo(singletonList("t"));
        assertThat(node.getSource().getKind()).isEqualTo(SqlKind.SELECT);
        assertThat(node.getTargetColumnList()).isNullOrEmpty();
    }

    private static SqlNode parse(String sql) throws SqlParseException {
        return SqlParser.create(sql, CONFIG).parseStmt();
    }
}
